from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib, sys, shapely, rasterio, netCDF4, datetime, georaster, requests, os, shutil, warnings, logging
import numpy as np
import rasterio.mask
import pandas as pd
import geopandas as gpd
import xarray as xr
from rasterio import features
from shapely.geometry import box

logging.basicConfig(filename='/home/ubuntu/subprocess/nasaaccess.log',level=logging.INFO)

def _rasterize_geom(geom, myshape, affinetrans, all_touched):
    indata = [(geom, 1)]
    rv_array = features.rasterize(
        indata,
        out_shape=myshape,
        transform=affinetrans,
        fill=0,
        all_touched=all_touched)
    return rv_array


def rasterize_pctcover(geom, atrans, myshape):
    alltouched = _rasterize_geom(geom, myshape, atrans, all_touched=True)
    exterior = _rasterize_geom(geom.exterior, myshape, atrans, all_touched=True)

    # Create percent cover grid as the difference between them
    # at this point all cells are known 100% coverage,
    # we'll update this array for exterior points
    pctcover = (alltouched - exterior) * 100

    # loop through indicies of all exterior cells
    for r, c in zip(*np.where(exterior == 1)):
        # Find cell bounds, from rasterio DatasetReader.window_bounds
        window = ((r, r + 1), (c, c + 1))
        ((row_min, row_max), (col_min, col_max)) = window
        x_min, y_min = (col_min, row_max) * atrans
        x_max, y_max = (col_max, row_min) * atrans
        bounds = (x_min, y_min, x_max, y_max)

        # Construct shapely geometry of cell
        cell = box(*bounds)

        # Intersect with original shape
        cell_overlap = cell.intersection(geom)

        # update pctcover with percentage based on area proportion
        coverage = cell_overlap.area / cell.area
        pctcover[r, c] = int(coverage * 100)

    return pctcover


def GLDASwat(Dir, watershed, DEM, start, end):
    #######Description

    # This function downloads remote sensing data of GLDAS from NASA GSFC servers, extracts air temperature data from grids within a specified watershed shapefile, and then generates tables in a format that SWAT requires for minimum and maximum air temperature data input.
    # The function also generates the air temperature stations file input (file with columns: ID, File NAME, LAT, LONG, and ELEVATION) for those selected grids that fall within the specified watershed.
    # The function assumes that users have already set up a registration account(s) with Earthdata login as well as authorizing NASA GESDISC data access. Please refer to  https://disc.gsfc.nasa.gov/data-access for further details.

    #######Arguments

    # Dir	A directory name to store gridded air temperature and air temperature stations files.
    # watershed	A study watershed shapefile spatially describing polygon(s) in a geographic projection sp::CRS('+proj=longlat +datum=WGS84').
    # DEM	A study watershed digital elevation model raster in a geographic projection sp::CRS('+proj=longlat +datum=WGS84').
    # start	Begining date for gridded air temperature data.
    # end	Ending date for gridded air temperature data.

    ######Details

    # A user should visit https://disc.gsfc.nasa.gov/data-access to register with the Earth Observing System Data and Information System (NASA Earthdata) and then authorize NASA GESDISC Data Access to successfuly work with this function. The function accesses NASA Goddard Space Flight Center server address for GLDAS remote sensing data products at (https://hydro1.gesdisc.eosdis.nasa.gov/data/GLDAS/GLDAS_NOAH025_3H.2.1/). The function uses varible name ('Tair_f_inst') for air temperature in GLDAS data products. Units for gridded air temperature data are degrees in 'K'. The GLDASwat function outputs gridded air temperature (maximum and minimum) data in degrees 'C'.

    # The goal of the Global Land Data Assimilation System GLDAS is to ingest satellite and ground-based observational data products, using advanced land surface modeling and data assimilation techniques, in order to generate optimal fields of land surface states and fluxes (Rodell et al., 2004). GLDAS dataset used in this function is the GLDAS Noah Land Surface Model L4 3 hourly 0.25 x 0.25 degree V2.1. The full suite of GLDAS datasets is avaliable at https://hydro1.gesdisc.eosdis.nasa.gov/dods/. The GLDASwat finds the minimum and maximum air temperatures for each day at each grid within the study watershed by searching for minima and maxima over the three hours air temperature data values available for each day and grid.

    # The GLDASwat function relies on 'curl' tool to transfer data from NASA servers to a user machine, using HTTPS supported protocol. The 'curl' command embedded in this function to fetch GLDAS netcdf daily global files is designed to work seamlessly given that appropriate logging information are stored in the ".netrc" file and the cookies file ".urs_cookies" as explained in registering with the Earth Observing System Data and Information System. It is imperative to say here that a user machine should have 'curl' installed as a prerequisite to run GLDASwat.

    # The GLDAS V2.1 simulation started on January 1, 2000 using the conditions from the GLDAS V2.0 simulation. The GLDAS V2.1 simulation was forced with National Oceanic and Atmospheric Administration NOAA, Global Data Assimilation System GDAS atmospheric analysis fields (Derber et al., 1991), the disaggregated Global Precipitation Climatology Project GPCP precipitation fields (Adler et al., 2003), and the Air Force Weather Agency’s AGRicultural METeorological modeling system AGRMET radiation fields which became available for March 1, 2001 onwards.

    ######Value

    # A table that includes points ID, Point file name, Lat, Long, and Elevation information formated to be read with SWAT, and a scalar of maximum and minimum air temperature gridded data values at each point within the study watershed in ascii format needed by SWAT model weather inputs will be stored at Dir.

    ######Note

    # start should be equal to or greater than 2000-Jan-01.

    ###Examples

    # GLDASwat(Dir = "./SWAT_INPUT/", watershed = "LowerMekong.shp",DEM = "LowerMekong_dem.tif", start = "2015-12-1", end = "2015-12-3")
    logging.info("Running GLDASwat")
    url_GLDAS_input = 'https://hydro1.gesdisc.eosdis.nasa.gov/data/GLDAS/GLDAS_NOAH025_3H.2.1/'
    myvar = 'Tair_f_inst'
    start = datetime.datetime.strptime(start, '%Y-%m-%d').date()
    end = datetime.datetime.strptime(end, '%Y-%m-%d').date()
    ####Before getting to work on this function do this check
    if start >= datetime.date(2000, 1, 1):
        # Constructing time series based on start and end input days!
        time_period = pd.date_range(start, end).tolist()
        # Reading cell elevation data (DEM should be in geographic projection)
        watershed_elevation = georaster.SingleBandRaster(DEM, load_data=False)
        # Reading the study Watershed shapefile
        polys = gpd.read_file(watershed)
        # extract the Watershed geometry in GeoJSON format
        geoms = polys.geometry.values  # list of shapely geometries
        geoms = [shapely.geometry.mapping(geoms[0])]
        # SWAT climate 'precipitation' master file name
        filenametableKEY = os.path.join(Dir, 'temp_Master.txt')
        # The GLDAS data grid information
        # Read start day to extract spatial information and assign elevation data to the grids within the study watersheds
        julianDate = start.strftime('%j')
        year = start.strftime('%Y')
        myurl = url_GLDAS_input + year + '/' + julianDate + '/'
        check1 = requests.get(myurl)
        if check1.status_code == 200:
            filenames = check1._content
            # getting the sub daily files at each juilan URL
            filenames = pd.read_html(filenames)[0][1][3]
            # Extract the GLDAS nc4 files for the specific day
            # downloading one file to be able writing Climate info table and gridded file names
            if not os.path.exists('./temp/'):
                os.makedirs('./temp/')
                os.chmod(os.path.join('./temp/'), 0o777)
                destfile = './temp/' + filenames
                filenames = myurl + filenames
                r = requests.get(filenames, stream=True)
                with open(destfile, 'wb') as fd:
                    os.chmod(os.path.join(destfile), 0o777)
                    fd.write(r.content)
                    fd.close()
                # reading ncdf file
                nc = netCDF4.Dataset(destfile)
                # since geographic info for all files are the same (assuming we are working with the same data product)
                ###evaluate these values one time!
                ###getting the y values (longitudes in degrees east)
                nc_long = nc.variables['lon'][:]
                ####getting the x values (latitudes in degrees north)
                nc_lat = nc.variables['lat'][:]
                ####getting the transform and resolutions for the IMERG raster data
                xres = (nc_long[-1] - nc_long[0]) / nc_long.shape[0]
                yres = (nc_lat[-1] - nc_lat[0]) / nc_lat.shape[0]
                transform_GLDAS = rasterio.transform.from_origin(west=nc_long[0], north=nc_lat[-1], xsize=xres,
                                                                 ysize=yres)
                # extract data
                data = nc.variables[myvar][0, :, :]  # Tair_f_inst(time,lat,lon)
                # close the netcdf file link
                nc.close()
                # save the daily climate data values in a raster
                temp_filename = './temp/' + 'temp_rough.tif'
                GLDAS = rasterio.open(temp_filename, 'w', driver='GTiff', height=data.shape[0], width=data.shape[1],
                                      count=1, dtype=data.dtype.name, crs=polys.crs, transform=transform_GLDAS)  #
                GLDAS.write(data, 1)
                GLDAS.close()
                # extract the raster x,y values within the watershed (polygon)
                with rasterio.open(temp_filename) as src:
                    out_image, out_transform = rasterio.mask.mask(src, geoms, all_touched=True, crop=True)
                # The out_image result is a Numpy masked array
                # no data values of the IMERG raster
                no_data = src.nodata
                # extract the values of the masked array
                data = out_image.data[0]
                # extract the row, columns of the valid values
                row, col = np.where(data != no_data)
                src.close()
                # Now get the coordinates of a cell center using affine transforms
                # Creation of a new resulting GeoDataFrame with the col, row and precipitation values
                d = gpd.GeoDataFrame({'NAME': 'temp', 'col': col, 'row': row}, crs=polys.crs)  #
                # lambda for evaluating raster data at cell center
                rc2xy = lambda r, c: (c, r) * T1
                T1 = out_transform * rasterio.Affine.translation(0.5, 0.5)  # reference the pixel center
                # coordinate transformation
                d['x'] = d.apply(lambda row: rc2xy(row.row, row.col)[0], axis=1)
                d['y'] = d.apply(lambda row: rc2xy(row.row, row.col)[1], axis=1)
                # geometry
                d['geometry'] = d.apply(lambda row: shapely.geometry.Point(row['x'], row['y']), axis=1)
                study_area_records = gpd.sjoin(d, polys, how='inner', op='intersects')
                ###working with DEM raster
                # lambda to evaluate elevation based on lat/long
                elev_x_y = lambda x, y: watershed_elevation.value_at_coords(x, y, latlon=True)
                study_area_records['ELEVATION'] = study_area_records.apply(lambda row: elev_x_y(row.x, row.y), axis=1)
                study_area_records = study_area_records.reset_index()
                study_area_records = study_area_records.rename(columns={'index': 'ID', 'x': 'LONG', 'y': 'LAT'})
                study_area_records['NAME'] = study_area_records['NAME'] + study_area_records['ID'].astype(str)
                # study_area_records.to_csv('./GLDAS_Table_result.txt',index=False)#
                shutil.rmtree('./temp/')
                del data, out_image, d, row, col, T1, nc_long, nc_lat, no_data, out_transform, temp_filename, destfile

                #### Begin writing SWAT climate input tables
                #### Get the SWAT file names and then put the first record date
                if not os.path.exists(Dir):
                    os.makedirs(Dir)
                    os.chmod(os.path.join(Dir), 0o777)
                    for h in range(study_area_records.shape[0]):
                        filenameSWAT_TXT = os.path.join(Dir, study_area_records['NAME'][h] + '.txt')
                        # write the data begining date once!
                        swat = open(filenameSWAT_TXT, 'w')  #
                        swat.write(format(time_period[0], '%Y%m%d'))
                        swat.write('\n')
                        swat.close()
                    #### Write out the SWAT grid information master table
                    OutSWAT = pd.DataFrame({'ID': study_area_records['ID'], 'NAME': study_area_records['NAME'],
                                            'LAT': study_area_records['LAT'], 'LONG': study_area_records['LONG'],
                                            'ELEVATION': study_area_records['ELEVATION']})
                    OutSWAT.to_csv(filenametableKEY, index=False)
                    #### Start doing the work!
                    #### iterate over days to extract records at GLDAS grids estabished in 'study_area_records'
                    for kk in range(len(time_period)):
                        julianDate = time_period[kk].strftime('%j')
                        year = time_period[kk].strftime('%Y')
                        myurl = url_GLDAS_input + year + '/' + julianDate + '/'
                        check2 = requests.get(myurl)
                        if check2.status_code == 200:
                            filenames = check2._content
                            # getting the subdaily files at each daily URL
                            filenames = pd.DataFrame({'Web File': pd.read_html(filenames)[0][1]})
                            filenames = filenames.dropna()
                            warnings.filterwarnings("ignore", 'This pattern has match groups')
                            criteria = filenames['Web File'].str.contains('GLDAS.+(.nc4$)')
                            filenames = filenames[criteria]
                            filenames = filenames.reset_index()
                            SubdailyTemp = np.empty([OutSWAT.shape[0], filenames.shape[0]])
                            # Extract the ncdf files
                            for gg in range(SubdailyTemp.shape[1]):  # Iterating over each subdaily data file
                                subdailyfilename = filenames['Web File'][gg]
                                if not os.path.exists('./temp/'):
                                    os.makedirs('./temp/')
                                    os.chmod(os.path.join('./temp/'), 0o777)
                                    destfile = './temp/' + subdailyfilename
                                    subdailyfilename = myurl + subdailyfilename
                                    r = requests.get(subdailyfilename, stream=True)
                                    with open(destfile, 'wb') as fd:
                                        os.chmod(os.path.join(destfile), 0o777)
                                        fd.write(r.content)
                                        fd.close()
                                        # reading ncdf file
                                        nc = xr.open_dataset(destfile)
                                        # looking only within the watershed
                                        nc = nc.merge(nc, geoms, join='inner')
                                        # evaluating climate at lat/lon points
                                        # Obtaining subdaily climate values at GLDAS grids that has been defined and explained earlier
                                        climate_values = nc.interp(lat=study_area_records['LAT'],
                                                                   lon=study_area_records['LONG'], method='nearest')
                                        SubdailyTemp[:, gg] = climate_values[myvar][0, :, :].data.diagonal()
                                        nc.close()
                                        shutil.rmtree('./temp/')
                            # obtain minimum daily data over the 3 hrs records
                            warnings.filterwarnings("ignore", 'All-NaN slice encountered')
                            mindailytemp = np.nanmin(SubdailyTemp, axis=1)  # removing missing data
                            mindailytemp = mindailytemp - 273.16  # convert to degree C
                            mindailytemp[np.where(np.isnan(mindailytemp))] = -99  # filing missing data
                            # same for maximum daily
                            warnings.filterwarnings("ignore", 'All-NaN slice encountered')
                            maxdailytemp = np.nanmax(SubdailyTemp, axis=1)
                            maxdailytemp = maxdailytemp - 273.16  # convert to degree C
                            maxdailytemp[np.where(np.isnan(maxdailytemp))] = -99  # filing missing data
                            # Looping through the GLDAS points and writing out the daily climate data in SWAT format
                            for h in range(study_area_records.shape[0]):
                                filenameSWAT_TXT = os.path.join(Dir, study_area_records['NAME'][h] + '.txt')
                                # write the data begining date once!
                                with open(filenameSWAT_TXT, 'a') as swat:
                                    # np.savetxt(swat, str(maxdailytemp[h]) + ',' + str(mindailytemp[h]))
                                    temptext = str(maxdailytemp[h]) + ',' + str(mindailytemp[h])
                                    swat.write(temptext)
                                    swat.write('\n')
                                    swat.close()
                            # shutil.rmtree('./temp/')
    else:
        print ('Sorry' + ", " + start.strftime("%b") + "-" + start.strftime(
            '%Y') + ' iis out of coverage for GLDAS data products.')
        print ('Please pick start date equal to or greater than 2000-Jan-01 to access GLDAS data products.')
        print ('Thank you!')


def GPMswat(Dir, watershed, DEM, start, end):
    ##########Description
    # This function downloads rainfall remote sensing data of TRMM and IMERG from NASA GSFC servers, extracts data from grids within a specified watershed shapefile, and then generates tables in a format that SWAT requires for rainfall data input.
    # The function also generates the rainfall stations file input (file with columns: ID, File NAME, LAT, LONG, and ELEVATION) for those selected grids that fall within the specified watershed.

    #########Arguments

    # Dir       A directory name to store gridded rainfall and rain stations files.
    # watershed	A study watershed shapefile spatially describing polygon(s) in a geographic projection sp::CRS('+proj=longlat +datum=WGS84').
    # DEM	A study watershed digital elevation model raster in a geographic projection sp::CRS('+proj=longlat +datum=WGS84').
    # start	Begining date for gridded rainfall data.
    # end	Ending date for gridded rainfall data.

    ########Details
    # A user should visit https://disc.gsfc.nasa.gov/data-access to register with the Earth Observing System Data and Information System (NASA Earthdata) and then authorize NASA GESDISC Data Access to successfuly work with this function. The function accesses NASA Goddard Space Flight Center server address for IMERG remote sensing data products at (https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDF.05/), and NASA Goddard Space Flight Center server address for TRMM remote sensing data products (https://disc2.gesdisc.eosdis.nasa.gov/data/TRMM_RT/TRMM_3B42RT_Daily.7). The function uses varible name ('precipitationCal') for rainfall in IMERG data products and variable name ('precipitation') for TRMM rainfall data products. Units for gridded rainfall data are 'mm'.

    # IMERG dataset is the GPM Level 3 IMERG *Final* Daily 0.1 x 0.1 deg (GPM_3IMERGDF) derived from the half-hourly GPM_3IMERGHH. The derived result represents the final estimate of the daily accumulated precipitation. The dataset is produced at the NASA Goddard Earth Sciences (GES) Data and Information Services Center (DISC) by simply summing the valid precipitation retrievals for the day in GPM_3IMERGHH and giving the result in (mm) https://pmm.nasa.gov/data-access/downloads/gpm.

    # TRMM dataset is a daily 0.25 x 0.25 deg accumulated precipitation product that is generated from the Near Real-Time 3-hourly TMPA (3B42RT). It is produced at the NASA GES DISC, as a value added product. Simple summation of valid retrievals in a grid cell is applied for the data day. The result is given in (mm) https://pmm.nasa.gov/data-access/downloads/trmm.

    # Since IMERG data products are only available from 2014-March-12 to present, then this function uses TRMM data products for time periods earlier than 2014-March-12. Keep in mind that TRMM data products that are compatible with IMERG data products are only available from 2000-March-01. The function outputs table and gridded data files that match grid points resolution of IMERG data products (i.e., resolution of 0.1 deg). Since TRMM and IMERG data products do not have a similar spatial resolution (i.e., 0.25 and 0.1 deg respectively), the function assigns the nearest TRMM grid point to any missing IMERG data point as an approximate (i.e. during 2000-March-01 to 2014-March-11 time period).

    # The GPMswat function relies on 'curl' tool to transfer data from NASA servers to a user machine, using HTTPS supported protocol. The 'curl' command embedded in this function to fetch precipitation IMERG/TRMM netcdf daily global files is designed to work seamlessly given that appropriate logging information are stored in the ".netrc" file and the cookies file ".urs_cookies" as explained in registering with the Earth Observing System Data and Information System. It is imperative to say here that a user machine should have 'curl' installed as a prerequisite to run GPMswat.

    ########Value
    # A table that includes points ID, Point file name, Lat, Long, and Elevation information formated to be read with SWAT, and a scalar of rainfall gridded data values at each point within the study watershed in ascii format needed by SWAT model weather inputs will be stored at Dir.

    #########Note
    # start should be equal to or greater than 2000-Mar-01.

    #######Example
    # GPMswat(Dir = "./SWAT_INPUT/", watershed = "LowerMekong.shp",DEM = "LowerMekong_dem.tif", start = "2015-12-1", end = "2015-12-3")

    logging.info("Running GPMSwat")
    url_IMERG_input = 'https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDF.05/'
    url_TRMM_input = 'https://disc2.gesdisc.eosdis.nasa.gov/data/TRMM_RT/TRMM_3B42RT_Daily.7/'
    myvarIMERG = 'precipitationCal'
    myvarTRMM = 'precipitation'
    start = datetime.datetime.strptime(start, '%Y-%m-%d').date()
    end = datetime.datetime.strptime(end, '%Y-%m-%d').date()
    ####Before getting to work on this function do this check
    if start >= datetime.date(2000, 3, 1):
        # Constructing time series based on start and end input days!
        time_period = pd.date_range(start, end).tolist()
        # Reading cell elevation data (DEM should be in geographic projection)
        watershed_elevation = georaster.SingleBandRaster(DEM, load_data=False)
        # Reading the study Watershed shapefile
        polys = gpd.read_file(watershed)
        # extract the Watershed geometry in GeoJSON format
        geoms = polys.geometry.values  # list of shapely geometries
        geoms = [shapely.geometry.mapping(geoms[0])]
        # SWAT climate 'precipitation' master file name
        filenametableKEY = Dir + myvarTRMM + 'Master.txt'
        # The IMERG data grid information
        # Read a dummy day to extract spatial information and assign elevation data to the grids within the study watersheds
        DUMMY_DATE = datetime.date(2014, 5, 1)
        mon = DUMMY_DATE.strftime('%m')
        year = DUMMY_DATE.strftime('%Y')
        myurl = url_IMERG_input + year + '/' + mon + '/'
        check1 = requests.get(myurl)
        if check1.status_code == 200:
            filenames = check1._content
            # getting one of the daily files at the monthly URL specified by DUMMY Date
            filenames = pd.read_html(filenames)[0][1][3]
            # Extract the IMERG nc4 files for the specific date
            # trying here the first day since I am only interested on grid locations
            # downloading one file
            if not os.path.exists('./temp/'):
                os.makedirs('./temp/')
                os.chmod(os.path.join('./temp/'), 0o777)
                destfile = './temp/' + filenames
                filenames = myurl + filenames
                r = requests.get(filenames, stream=True)
                with open(destfile, 'wb') as fd:
                    os.chmod(os.path.join(destfile), 0o777)
                    fd.write(r.content)
                    fd.close()
                # reading ncdf file
                nc = netCDF4.Dataset(destfile)
                # since geographic info for all files are the same (assuming we are working with the same data product)
                ###evaluate these values one time!
                ###getting the y values (longitudes in degrees east)
                nc_long_IMERG = nc.variables['lon'][:]
                ####getting the x values (latitudes in degrees north)
                nc_lat_IMERG = nc.variables['lat'][:]
                ####getting the transform and resolutions for the IMERG raster data
                xres_IMERG = (nc_long_IMERG[-1] - nc_long_IMERG[0]) / nc_long_IMERG.shape[0]
                yres_IMERG = (nc_lat_IMERG[-1] - nc_lat_IMERG[0]) / nc_lat_IMERG.shape[0]
                transform_IMERG = rasterio.transform.from_origin(west=nc_long_IMERG[0], north=nc_lat_IMERG[-1],
                                                                 xsize=xres_IMERG, ysize=yres_IMERG)
                # extract data
                data = nc.variables[myvarIMERG][:]
                # reorder the rows
                data = np.transpose(data)
                # close the netcdf file link
                nc.close()
                # save the daily climate data values in a raster
                IMERG_temp_filename = './temp/' + 'pcp_rough.tif'
                IMERG = rasterio.open(IMERG_temp_filename, 'w', driver='GTiff', height=data.shape[0],
                                      width=data.shape[1], count=1, dtype=data.dtype.name, crs=polys.crs,
                                      transform=transform_IMERG)  #
                IMERG.write(data, 1)
                IMERG.close()
                # extract the raster x,y values within the watershed (polygon)
                with rasterio.open(IMERG_temp_filename) as src:
                    out_image, out_transform = rasterio.mask.mask(src, geoms, all_touched=True, crop=True)
                # The out_image result is a Numpy masked array
                # no data values of the IMERG raster
                no_data = src.nodata
                # extract the values of the masked array
                data = out_image.data[0]
                # extract the row, columns of the valid values
                row, col = np.where(data != no_data)
                # Pcp = np.extract(data != no_data, data)
                # polys_crs_wkt = src.crs.wkt
                src.close()
                # Now get the coordinates of a cell center using affine transforms
                # Creation of a new resulting GeoDataFrame with the col, row and precipitation values
                d = gpd.GeoDataFrame({'NAME': myvarTRMM, 'col': col, 'row': row}, crs=polys.crs)  #
                # lambda for evaluating raster data at cell center
                rc2xy = lambda r, c: (c, r) * T1
                T1 = out_transform * rasterio.Affine.translation(0.5, 0.5)  # reference the pixel center
                # coordinate transformation
                d['x'] = d.apply(lambda row: rc2xy(row.row, row.col)[0], axis=1)
                d['y'] = d.apply(lambda row: rc2xy(row.row, row.col)[1], axis=1)
                # geometry
                d['geometry'] = d.apply(lambda row: shapely.geometry.Point(row['x'], row['y']), axis=1)
                study_area_records_IMERG = gpd.sjoin(d, polys, how='inner', op='intersects')
                ###working with DEM raster
                # lambda to evaluate elevation based on lat/long
                elev_x_y = lambda x, y: watershed_elevation.value_at_coords(x, y, latlon=True)
                study_area_records_IMERG['ELEVATION'] = study_area_records_IMERG.apply(
                    lambda row: elev_x_y(row.x, row.y), axis=1)
                study_area_records_IMERG = study_area_records_IMERG.reset_index()
                study_area_records_IMERG = study_area_records_IMERG.rename(
                    columns={'index': 'ID', 'x': 'LONG', 'y': 'LAT'})
                study_area_records_IMERG['NAME'] = study_area_records_IMERG['NAME'] + study_area_records_IMERG[
                    'ID'].astype(str)
                # study_area_records_IMERG.to_csv('./IMERG_Table_result.txt',index=False)#
                shutil.rmtree('./temp/')
                del data, out_image, d, row, col, nc_long_IMERG, T1, nc_lat_IMERG, no_data, out_transform, IMERG_temp_filename, destfile
                # The TRMM data grid information
                # Use the same dummy date defined above since TRMM has data up to present with less accurancy. The recomendation is to use IMERG data from 2014-03-12 and onward!
                # update my url with TRMM information
                myurl = url_TRMM_input + year + '/' + mon + '/'
                check2 = requests.get(myurl)
                if check2.status_code == 200:
                    filenames = check2._content
                    # getting one of the daily files at the monthly URL specified by DUMMY Date
                    filenames = pd.read_html(filenames)[0][1][3]
                    # Extract the TRMM nc4 files for the specific month
                    # trying here the first day since I am only interested on grid locations
                    # downloading one file
                    if not os.path.exists('./temp/'):
                        os.makedirs('./temp/')
                        os.chmod(os.path.join('./temp'), 0o777)
                        destfile = './temp/' + filenames
                        filenames = myurl + filenames
                        r = requests.get(filenames, stream=True)
                        with open(destfile, 'wb') as fd:
                            os.chmod(os.path.join(destfile), 0o777)
                            fd.write(r.content)
                            fd.close()
                        # reading ncdf file
                        nc = netCDF4.Dataset(destfile, mode='r')
                        ###evaluate these values one time!
                        ###getting the y values (longitudes in degrees east)
                        nc_long_TRMM = nc.variables['lon'][:]
                        ####getting the x values (latitudes in degrees north)
                        nc_lat_TRMM = nc.variables['lat'][:]
                        ####getting the transform and resolutions for the IMERG raster data
                        xres_TRMM = (nc_long_TRMM[-1] - nc_long_TRMM[0]) / nc_long_TRMM.shape[0]
                        yres_TRMM = (nc_lat_TRMM[-1] - nc_lat_TRMM[0]) / nc_lat_TRMM.shape[0]
                        transform_TRMM = rasterio.transform.from_origin(west=nc_long_TRMM[0], north=nc_lat_TRMM[-1],
                                                                        xsize=xres_TRMM, ysize=yres_TRMM)
                        # extract data
                        data = nc.variables[myvarTRMM][:]
                        # reorder the rows
                        data = np.transpose(data)
                        # close the netcdf file link
                        nc.close()
                        # save the daily climate data values in a raster
                        TRMM_temp_filename = './temp/' + 'pcp_trmm_rough.tif'
                        TRMM = rasterio.open(TRMM_temp_filename, 'w', driver='GTiff', height=data.shape[0],
                                             width=data.shape[1], count=1, dtype=data.dtype.name, crs=polys.crs,
                                             transform=transform_TRMM)  #
                        TRMM.write(data, 1)
                        TRMM.close()
                        # extract the raster x,y values within the watershed (polygon)
                        with rasterio.open(TRMM_temp_filename) as src:
                            out_image, out_transform = rasterio.mask.mask(src, geoms, all_touched=True, crop=True)
                        # The out_image result is a Numpy masked array
                        # no data values of the TRMM raster
                        no_data = src.nodata
                        # extract the values of the masked array
                        data = out_image.data[0]
                        # extract the row, columns of the valid values
                        row, col = np.where(data != no_data)
                        src.close()
                        # Now I use How to I get the coordinates of a cell in a geotif? or Python affine transforms to transform between the pixel and projected coordinates with out_transform as the affine transform for the subset data
                        rc2xy = lambda r, c: (c, r) * T1
                        T1 = out_transform * rasterio.Affine.translation(0.5, 0.5)  # reference the pixel center
                        # Creation of a new resulting GeoDataFrame with the col, row and precipitation values
                        d = gpd.GeoDataFrame({'NAME': myvarTRMM, 'col': col, 'row': row}, crs=polys.crs)  #
                        # coordinate transformation
                        d['x'] = d.apply(lambda row: rc2xy(row.row, row.col)[0], axis=1)
                        d['y'] = d.apply(lambda row: rc2xy(row.row, row.col)[1], axis=1)
                        # geometry
                        d['geometry'] = d.apply(lambda row: shapely.geometry.Point(row['x'], row['y']), axis=1)
                        study_area_records_TRMM = gpd.sjoin(d, polys, how='inner', op='intersects')
                        study_area_records_TRMM = study_area_records_TRMM.reset_index()
                        study_area_records_TRMM = study_area_records_TRMM.rename(
                            columns={'index': 'TRMMiD', 'x': 'TRMMlONG', 'y': 'TRMMlAT'})
                        study_area_records_TRMM['NAME'] = study_area_records_TRMM['NAME'] + study_area_records_TRMM[
                            'TRMMiD'].astype(str)
                        # study_area_records_TRMM.to_csv('./TRMM_Table_result.txt',index=False)#
                        # study_area_records_TRMM[['TRMMiD','NAME','TRMMlONG','TRMMlAT','geometry']].to_file('./TRMM_Pcp_result.shp', driver='ESRI Shapefile',crs_wkt=polys_crs_wkt)#
                        shutil.rmtree('./temp/')
                        del data, out_image, d, row, col, T1, nc_long_TRMM, nc_lat_TRMM, no_data, out_transform, TRMM_temp_filename, destfile
                        # creating a similarity table that connects IMERG and TRMM grids
                        # calculate euclidean distances to know how to connect TRMM grids with IMERG grids
                        ee = pd.DataFrame(columns=study_area_records_TRMM.columns.values)
                        for i in range(study_area_records_IMERG.shape[0]):
                            study_area_records_TRMM['distVec'] = study_area_records_TRMM['geometry'].distance(
                                study_area_records_IMERG['geometry'][i])
                            ff = study_area_records_TRMM[
                                (study_area_records_TRMM.distVec <= study_area_records_TRMM.distVec.min())]
                            ee = ee.append(ff, ignore_index=True, sort=True)
                        ee = ee.rename(columns={'TRMMiD': 'CloseTRMMIndex', 'col': 'TRMMcol', 'row': 'TRMMrow'})
                        FinalTable = pd.DataFrame(
                            {'ID': study_area_records_IMERG['ID'], 'NAME': study_area_records_IMERG['NAME'],
                             'LONG': study_area_records_IMERG['LONG'], 'LAT': study_area_records_IMERG['LAT'],
                             'ELEVATION': study_area_records_IMERG['ELEVATION'], 'CloseTRMMIndex': ee['CloseTRMMIndex'],
                             'TRMMlONG': ee['TRMMlONG'], 'TRMMlAT': ee['TRMMlAT'], 'TRMMrow': ee['TRMMrow'],
                             'TRMMcol': ee['TRMMcol']})
                        # FinalTable.to_csv('./FinalTable.txt',index=False)#
                        #### Begin writing SWAT climate input tables
                        #### Get the SWAT file names and then put the first record date
                        if not os.path.exists(Dir):
                            os.makedirs(Dir)
                            os.chmod(os.path.join(Dir), 0o777)
                            for h in range(FinalTable.shape[0]):
                                filenameSWAT_TXT = Dir + FinalTable['NAME'][h] + '.txt'
                                # write the data begining date once!
                                swat = open(filenameSWAT_TXT, 'w')  #
                                swat.write(format(time_period[0], '%Y%m%d'))
                                swat.write('\n')
                                swat.close()
                            #### Write out the SWAT grid information master table
                            OutSWAT = pd.DataFrame(
                                {'ID': FinalTable['ID'], 'NAME': FinalTable['NAME'], 'LAT': FinalTable['LAT'],
                                 'LONG': FinalTable['LONG'], 'ELEVATION': FinalTable['ELEVATION']})
                            OutSWAT.to_csv(filenametableKEY, index=False)
                            #### Start doing the work!
                            #### iterate over days to extract record at IMERG grids estabished in 'FinalTable'
                            for kk in range(len(time_period)):
                                mon = time_period[kk].strftime('%m')
                                year = time_period[kk].strftime('%Y')
                                # Decide here whether to use TRMM or IMERG based on data availability
                                # Begin with TRMM first which means days before 2014-March-12
                                if time_period[kk].date() < datetime.date(2014, 3, 12):
                                    myurl = url_TRMM_input + year + '/' + mon + '/'
                                    check3 = requests.get(myurl)
                                    if check3.status_code == 200:
                                        filenames = check3._content
                                        # getting the daily files at each monthly URL
                                        filenames = pd.DataFrame({'Web File': pd.read_html(filenames)[0][1]})
                                        filenames = filenames.dropna()
                                        warnings.filterwarnings("ignore", 'This pattern has match groups')
                                        criteria = filenames['Web File'].str.contains('3B42.+(.nc4$)')
                                        filenames = filenames[criteria]
                                        filenames['Date'] = filenames['Web File'].str.extract('(\d\d\d\d\d\d\d\d)',
                                                                                              expand=True)
                                        filenames['Date'] = pd.to_datetime(filenames['Date'], format='%Y%m%d',
                                                                           errors='coerce')
                                        filenames = filenames[filenames['Date'] == time_period[kk]]
                                        if not os.path.exists('./temp/'):
                                            os.makedirs('./temp/')
                                            os.chmod(os.path.join('./temp/'), 0o777)
                                        destfile = './temp/' + filenames['Web File'].values[0]
                                        filenames = myurl + filenames['Web File'].values[0]
                                        r = requests.get(filenames, stream=True)
                                        with open(destfile, 'wb') as fd:
                                            os.chmod(os.path.join(destfile), 0o777)
                                            fd.write(r.content)
                                            fd.close()
                                            # reading ncdf file
                                            nc = xr.open_dataset(destfile)
                                            # looking only within the watershed
                                            nc = nc.merge(nc, geoms, join='inner')
                                            # evaluating precipitation at lat/lon points
                                            pcp_values = nc.interp(lon=FinalTable['TRMMlONG'],
                                                                   lat=FinalTable['TRMMlAT'], method='nearest')
                                            FinalTable['cell_values'] = pcp_values[myvarTRMM].data.diagonal()
                                            FinalTable['cell_values'] = FinalTable['cell_values'].fillna(-99.0)
                                            ### Looping through the IMERG points and writing out the daily climate data in SWAT format
                                            for h in range(FinalTable.shape[0]):
                                                filenameSWAT_TXT = Dir + FinalTable['NAME'][h] + '.txt'
                                                # write the data begining date once!
                                                with open(filenameSWAT_TXT, 'a') as swat:
                                                    np.savetxt(swat, [FinalTable['cell_values'].values[h]])
                                            shutil.rmtree('./temp/')
                                            ## Now for dates equal to or greater than 2014 March 12 (i.e., IMERG)
                                else:
                                    myurl = url_IMERG_input + year + '/' + mon + '/'
                                    check4 = requests.get(myurl)
                                    if check4.status_code == 200:
                                        filenames = check4._content
                                        # getting the daily files at each monthly URL
                                        filenames = pd.DataFrame({'Web File': pd.read_html(filenames)[0][1]})
                                        filenames = filenames.dropna()
                                        warnings.filterwarnings("ignore", 'This pattern has match groups')
                                        criteria = filenames['Web File'].str.contains('3B-DAY.+(.nc4$)')
                                        filenames = filenames[criteria]
                                        filenames['Date'] = filenames['Web File'].str.extract('(\d\d\d\d\d\d\d\d)',
                                                                                              expand=True)
                                        filenames['Date'] = pd.to_datetime(filenames['Date'], format='%Y%m%d',
                                                                           errors='coerce')
                                        filenames = filenames[filenames['Date'] == time_period[kk]]
                                        if not os.path.exists('./temp/'):
                                            os.makedirs('./temp/')
                                            os.chmod(os.path.join('./temp/'), 0o777)
                                            destfile = './temp/' + filenames['Web File'].values[0]
                                            filenames = myurl + filenames['Web File'].values[0]
                                            r = requests.get(filenames, stream=True)
                                            with open(destfile, 'wb') as fd:
                                                os.chmod(os.path.join(destfile), 0o777)
                                                fd.write(r.content)
                                                fd.close()
                                            # reading ncdf file
                                            nc = xr.open_dataset(destfile)
                                            # looking only within the watershed
                                            nc = nc.merge(nc, geoms, join='inner')
                                            # evaluating precipitation at lat/lon points
                                            pcp_values = nc.interp(lon=FinalTable['LONG'], lat=FinalTable['LAT'],
                                                                   method='nearest')
                                            FinalTable['cell_values'] = pcp_values[myvarIMERG].data.diagonal()
                                            FinalTable['cell_values'] = FinalTable['cell_values'].fillna(-99.0)
                                            ### Looping through the IMERG points and writing out the daily climate data in SWAT format
                                            for h in range(FinalTable.shape[0]):
                                                filenameSWAT_TXT = Dir + FinalTable['NAME'][h] + '.txt'
                                                # write the data begining date once!
                                                with open(filenameSWAT_TXT, 'a') as swat:
                                                    np.savetxt(swat, [FinalTable['cell_values'].values[h]])
                                            shutil.rmtree('./temp/')

    else:
        print ('Sorry' + ", " + start.strftime("%b") + "-" + start.strftime(
            '%Y') + ' is out of coverage for TRMM or IMERG data products.')
        print ('Please pick start date equal to or greater than 2000-Mar-01 to access TRMM and IMERG data products.')
        print ('Thank you!')
        logging.info("Dates are not valid")


def GPMpolyCentroid(Dir, watershed, DEM, start, end):
    ###Description

    # This function downloads rainfall remote sensing data of TRMM and IMERG from NASA GSFC servers, extracts data from grids falling within a specified sub-basin(s) watershed shapefile and assigns a pseudo rainfall gauge located at the centeroid of the sub-basin(s) watershed a weighted-average daily rainfall data.
    # The function generates rainfall tables in a format that SWAT or other rainfall-runoff hydrological model requires for rainfall data input.
    # The function also generates the rainfall stations file summary input (file with columns: ID, File NAME, LAT, LONG, and ELEVATION) for those pseudo grids that correspond to the centroids of the watershed sub-basins.
    # The function assumes that users have already set up a registration account(s) with Earthdata login as well as authorizing NASA GESDISC data access. Please refer to  https://disc.gsfc.nasa.gov/data-access for further details.

    #######Arguments

    # Dir	A directory name to store gridded air temperature and air temperature stations files.
    # watershed	A study watershed shapefile spatially describing polygon(s) in a geographic projection sp::CRS('+proj=longlat +datum=WGS84').
    # DEM	A study watershed digital elevation model raster in a geographic projection sp::CRS('+proj=longlat +datum=WGS84').
    # start	Begining date for gridded air temperature data.
    # end	Ending date for gridded air temperature data.

    #######Details
    # A user should visit https://disc.gsfc.nasa.gov/data-access to register with the Earth Observing System Data and Information System (NASA Earthdata) and then authorize NASA GESDISC Data Access to successfuly work with this function. The function accesses NASA Goddard Space Flight Center server address for IMERG remote sensing data products at (https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDF.05/), and NASA Goddard Space Flight Center server address for TRMM remote sensing data products (https://disc2.gesdisc.eosdis.nasa.gov/data/TRMM_RT/TRMM_3B42RT_Daily.7). The function uses varible name ('precipitationCal') for rainfall in IMERG data products and variable name ('precipitation') for TRMM rainfall data products. Units for gridded rainfall data are 'mm'.

    # IMERG dataset is the GPM Level 3 IMERG *Final* Daily 0.1 x 0.1 deg (GPM_3IMERGDF) derived from the half-hourly GPM_3IMERGHH. The derived result represents the final estimate of the daily accumulated precipitation. The dataset is produced at the NASA Goddard Earth Sciences (GES) Data and Information Services Center (DISC) by simply summing the valid precipitation retrievals for the day in GPM_3IMERGHH and giving the result in (mm) https://pmm.nasa.gov/data-access/downloads/gpm.

    # TRMM dataset is a daily 0.25 x 0.25 deg accumulated precipitation product that is generated from the Near Real-Time 3-hourly TMPA (3B42RT). It is produced at the NASA GES DISC, as a value added product. Simple summation of valid retrievals in a grid cell is applied for the data day. The result is given in (mm) https://pmm.nasa.gov/data-access/downloads/trmm.

    # Since IMERG data products are only available from 2014-March-12 to present, then this function uses TRMM data products for time periods earlier than 2014-March-12. Keep in mind that TRMM data products that are compatible with IMERG data products are only available from 2000-March-01. The function outputs table and gridded data files that match grid points resolution of IMERG data products (i.e., resolution of 0.1 deg). Since TRMM and IMERG data products do not have a similar spatial resolution (i.e., 0.25 and 0.1 deg respectively), the function assigns the nearest TRMM grid point to any missing IMERG data point as an approximate (i.e. during 2000-March-01 to 2014-March-11 time period).

    #######Value
    # A table that includes Points ID, Point file name, Lat, Long, and Elevation information formated to be read with SWAT or other hydrological model, and a scalar of rainfall gridded data values at a pseudo rain grid located at the centeroid of each sub-basin within the study watershed provided in ascii format needed by SWAT model or other hydrological model weather inputs. All rain tables will be stored at Dir.

    ######Note

    # start should be equal to or greater than 2000-Mar-01.

    ###Examples
    # GPMpolyCentroid(Dir = "./SWAT_INPUT/", watershed = "LowerMekong.shp", DEM = "LowerMekong_dem.tif", start = "2015-12-1", end = "2015-12-3")
    logging.info("Running GPMpolycentroid")
    url_IMERG_input = 'https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDF.05/'
    url_TRMM_input = 'https://disc2.gesdisc.eosdis.nasa.gov/data/TRMM_RT/TRMM_3B42RT_Daily.7/'
    myvarIMERG = 'precipitationCal'
    myvarTRMM = 'precipitation'
    start = datetime.datetime.strptime(start, '%Y-%m-%d').date()
    end = datetime.datetime.strptime(end, '%Y-%m-%d').date()
    ####Before getting to work on this function do this check
    if start >= datetime.date(2000, 3, 1):
        transform_TRMM = None
        transform_IMERG = None
        # SWAT climate 'precipitation' master file name
        filenametableKEY = Dir + myvarTRMM + 'Master.txt'
        # Constructing time series based on start and end input days!
        time_period = pd.date_range(start, end).tolist()
        # Reading cell elevation data (DEM should be in geographic projection)
        watershed_elevation = georaster.SingleBandRaster(DEM, load_data=False)
        # Reading the study Watershed shapefile
        polys = gpd.read_file(watershed)
        subbasinCentroids = polys.geometry.centroid
        # Creation of a new resulting GeoDataFrame with the col, row and temperature values
        study_area_records = gpd.GeoDataFrame({'NAME': 'precip', 'geometry': subbasinCentroids}, crs=polys.crs)  #
        study_area_records['LONG'] = study_area_records.geometry.x
        study_area_records['LAT'] = study_area_records.geometry.y
        # lambda to evaluate elevation based on lat/long
        elev_x_y = lambda x, y: watershed_elevation.value_at_coords(x, y, latlon=True)
        study_area_records['ELEVATION'] = study_area_records.apply(lambda row: elev_x_y(row.LONG, row.LAT), axis=1)
        study_area_records = study_area_records.reset_index()
        study_area_records = study_area_records.rename(columns={'index': 'ID'})
        study_area_records['NAME'] = study_area_records['NAME'] + study_area_records['ID'].astype(str)
        #### Begin writing SWAT climate input tables
        #### Get the SWAT file names and then put the first record date
        if not os.path.exists(Dir):
            os.makedirs(Dir)
            os.chmod(os.path.join(Dir), 0o777)
        for h in range(study_area_records.shape[0]):
            filenameSWAT_TXT = Dir + study_area_records['NAME'][h] + '.txt'
            # write the data begining date once!
            swat = open(filenameSWAT_TXT, 'w')  #
            swat.write(format(time_period[0], '%Y%m%d'))
            swat.write('\n')
            swat.close()
        # Write out the SWAT grid information master table
        OutSWAT = pd.DataFrame(
            {'ID': study_area_records['ID'], 'NAME': study_area_records['NAME'], 'LAT': study_area_records['LAT'],
             'LONG': study_area_records['LONG'], 'ELEVATION': study_area_records['ELEVATION']})
        OutSWAT.to_csv(filenametableKEY, index=False)
        #### Start doing the work!
        #### iterate over days to extract records at TRMM or IMERG grids estabished in 'study_area_records'
        for kk in range(len(time_period)):
            mon = time_period[kk].strftime('%m')
            year = time_period[kk].strftime('%Y')
            # Decide here whether to use TRMM or IMERG based on data availability
            # Begin with TRMM first which means days before 2014-March-12
            if time_period[kk].date() < datetime.date(2014, 3, 12):
                myurl = url_TRMM_input + year + '/' + mon + '/'
                check1 = requests.get(myurl)
                if check1.status_code == 200:
                    filenames = check1._content
                    # getting the daily files at each monthly URL
                    filenames = pd.DataFrame({'Web File': pd.read_html(filenames)[0][1]})
                    filenames = filenames.dropna()
                    warnings.filterwarnings("ignore", 'This pattern has match groups')
                    criteria = filenames['Web File'].str.contains('3B42.+(.nc4$)')
                    filenames = filenames[criteria]
                    filenames['Date'] = filenames['Web File'].str.extract('(\d\d\d\d\d\d\d\d)', expand=True)
                    filenames['Date'] = pd.to_datetime(filenames['Date'], format='%Y%m%d', errors='coerce')
                    filenames = filenames[filenames['Date'] == time_period[kk]]
                    dailyPrecip = np.zeros([OutSWAT.shape[0]])
                    if not os.path.exists('./temp/'):
                        os.makedirs('./temp/')
                        os.chmod(os.path.join('./temp/'), 0o777)
                    destfile = './temp/' + filenames['Web File'].values[0]
                    filenames = myurl + filenames['Web File'].values[0]
                    r = requests.get(filenames, stream=True)
                    with open(destfile, 'wb') as fd:
                        os.chmod(os.path.join(destfile), 0o777)
                        fd.write(r.content)
                        fd.close()
                    # reading ncdf file
                    nc = xr.open_dataset(destfile)
                    data = nc.variables[myvarTRMM].data
                    # nc = netCDF4.Dataset(destfile)
                    # data = nc.variables[myvarTRMM][:,:].data
                    # reorder the rows
                    data = np.flip(np.transpose(data), axis=0)
                    # data = np.flip(data,axis=0)
                    # nodata = nc.missing_value
                    # ind = np.where(data==nodata)
                    # data[ind] = None
                    # calculate the TRMM Affine tranform and zonal weights once!
                    if transform_TRMM == None:
                        ###getting the y values (longitudes in degrees east)
                        nc_long = nc.variables['lon']
                        ####getting the x values (latitudes in degrees north)
                        nc_lat = nc.variables['lat']
                        ####getting the transform and resolutions for the IMERG raster data
                        xres = (nc_long[-1].values - nc_long[0].values) / nc_long.shape[0]
                        yres = (nc_lat[-1].values - nc_lat[0].values) / nc_lat.shape[0]
                        transform_TRMM = rasterio.transform.from_origin(west=nc_long[0].values, north=nc_lat[-1].values,
                                                                        xsize=xres, ysize=yres)
                        ###calculating the weights for zonal average
                        TRMMweights = np.zeros(
                            shape=(polys.shape[0], nc.variables['lat'].shape[0], nc.variables['lon'].shape[0]))
                        myshape = (nc.variables['lat'].shape[0], nc.variables['lon'].shape[0])
                        for mm in range(polys.shape[0]):
                            geom = polys['geometry'][mm]
                            if geom.geometryType() == 'MultiPolygon':
                                geom = geom.convex_hull
                            TRMMweights[mm, :, :] = rasterize_pctcover(geom, transform_TRMM, myshape) / 100
                    nc.close()
                    for ee in range(OutSWAT.shape[0]):
                        climate_values = TRMMweights[ee, :, :] * data
                        warnings.filterwarnings("ignore", 'invalid value encountered in double_scalars')
                        pcp_day = np.nansum(climate_values) / np.nansum(TRMMweights[ee, :, :])
                        if np.isnan(pcp_day):
                            pcp_day = -99
                        dailyPrecip[ee] = pcp_day
                        # Looping through the TRMM points and writing out the daily climate data in SWAT format
                    for h in range(OutSWAT.shape[0]):
                        filenameSWAT_TXT = Dir + study_area_records['NAME'][h] + '.txt'
                        # write the data begining date once!
                        with open(filenameSWAT_TXT, 'a') as swat:
                            swat.write(str(dailyPrecip[h]))
                            swat.write('\n')
                            swat.close()
                    shutil.rmtree('./temp/')
            else:  ## Now for dates equal to or greater than 2014 March 12
                myurl = url_IMERG_input + year + '/' + mon + '/'
                check2 = requests.get(myurl)
                if check2.status_code == 200:
                    filenames = check2._content
                    # getting the daily files at each monthly URL
                    filenames = pd.DataFrame({'Web File': pd.read_html(filenames)[0][1]})
                    filenames = filenames.dropna()
                    warnings.filterwarnings("ignore", 'This pattern has match groups')
                    criteria = filenames['Web File'].str.contains('3B-DAY.+(.nc4$)')
                    filenames = filenames[criteria]
                    filenames['Date'] = filenames['Web File'].str.extract('(\d\d\d\d\d\d\d\d)', expand=True)
                    filenames['Date'] = pd.to_datetime(filenames['Date'], format='%Y%m%d', errors='coerce')
                    filenames = filenames[filenames['Date'] == time_period[kk]]
                    dailyPrecip = np.zeros([OutSWAT.shape[0]])
                    if not os.path.exists('./temp/'):
                        os.makedirs('./temp/')
                        os.chmod(os.path.join('./temp/'), 0o777)
                    destfile = './temp/' + filenames['Web File'].values[0]
                    filenames = myurl + filenames['Web File'].values[0]
                    r = requests.get(filenames, stream=True)
                    with open(destfile, 'wb') as fd:
                        os.chmod(os.path.join(destfile), 0o777)
                        fd.write(r.content)
                        fd.close()
                    # reading ncdf file
                    nc = xr.open_dataset(destfile)
                    data = nc.variables[myvarIMERG].data
                    # reorder the rows
                    data = np.flip(np.transpose(data), axis=0)
                    # nodata = nc.variables[myvarIMERG]._FillValue
                    # ind = np.where(data==nodata)
                    # data[ind] = None
                    # calculate the IMERG Affine tranform and zonal weights once!
                    if transform_IMERG == None:
                        ###getting the y values (longitudes in degrees east)
                        nc_long = nc.variables['lon']
                        ####getting the x values (latitudes in degrees north)
                        nc_lat = nc.variables['lat']
                        ####getting the transform and resolutions for the IMERG raster data
                        xres = (nc_long[-1].values - nc_long[0].values) / nc_long.shape[0]
                        yres = (nc_lat[-1].values - nc_lat[0].values) / nc_lat.shape[0]
                        transform_IMERG = rasterio.transform.from_origin(west=nc_long[0].values,
                                                                         north=nc_lat[-1].values, xsize=xres,
                                                                         ysize=yres)
                        ###calculating the weights for zonal average
                        IMERGweights = np.zeros(
                            shape=(polys.shape[0], nc.variables['lat'].shape[0], nc.variables['lon'].shape[0]))
                        myshape = (nc.variables['lat'].shape[0], nc.variables['lon'].shape[0])
                        for mm in range(polys.shape[0]):
                            geom = polys['geometry'][mm]
                            if geom.geometryType() == 'MultiPolygon':
                                geom = geom.convex_hull
                            IMERGweights[mm, :, :] = rasterize_pctcover(geom, transform_IMERG, myshape) / 100

                    for ee in range(OutSWAT.shape[0]):
                        climate_values = IMERGweights[ee, :, :] * data
                        warnings.filterwarnings("ignore", 'invalid value encountered in double_scalars')
                        pcp_day = np.nansum(climate_values) / np.nansum(IMERGweights[ee, :, :])
                        if np.isnan(pcp_day):
                            pcp_day = -99
                        dailyPrecip[ee] = pcp_day
                    # Looping through the TRMM points and writing out the daily climate data in SWAT format
                    for h in range(OutSWAT.shape[0]):
                        filenameSWAT_TXT = Dir + study_area_records['NAME'][h] + '.txt'
                        # write the data begining date once!
                        with open(filenameSWAT_TXT, 'a') as swat:
                            swat.write(str(dailyPrecip[h]))
                            swat.write('\n')
                            swat.close()
                    nc.close()
                    shutil.rmtree('./temp/')

    else:
        print ('Sorry' + ", " + start.strftime("%b") + "-" + start.strftime(
            '%Y') + ' is out of coverage for TRMM or IMERG data products.')
        print ('Please pick start date equal to or greater than 2000-Mar-01 to access TRMM and IMERG data products.')
        print ('Thank you!')


def GLDASpolyCentroid(Dir, watershed, DEM, start, end):
    ###Description

    # This function downloads remote sensing data of GLDAS from NASA GSFC servers, extracts air temperature data from grids falling within a specified sub-basin(s) watershed shapefile, and assigns a pseudo air temperature gauge located at the centeroid of the sub-basin(s) watershed a weighted-average daily minimum and maximum air temperature data.
    # The function generates tables in a format that SWAT or other rainfall-runoff hydrological model requires for minimum and maximum air temperatures data input.
    # The function also generates the air temperature stations file input (file with columns: ID, File NAME, LAT, LONG, and ELEVATION) for those selected grids that pseudo grids that correspond to the centroids of the watershed sub-basins.
    # The function assumes that users have already set up a registration account(s) with Earthdata login as well as authorizing NASA GESDISC data access. Please refer to  https://disc.gsfc.nasa.gov/data-access for further details.

    #######Arguments

    # Dir	A directory name to store gridded air temperature and air temperature stations files.
    # watershed	A study watershed shapefile spatially describing polygon(s) in a geographic projection sp::CRS('+proj=longlat +datum=WGS84').
    # DEM	A study watershed digital elevation model raster in a geographic projection sp::CRS('+proj=longlat +datum=WGS84').
    # start	Begining date for gridded air temperature data.
    # end	Ending date for gridded air temperature data.

    #######Details

    # A user should visit https://disc.gsfc.nasa.gov/data-access to register with the Earth Observing System Data and Information System (NASA Earthdata) and then authorize NASA GESDISC Data Access to successfuly work with this function. The function accesses NASA Goddard Space Flight Center server address for GLDAS remote sensing data products at (https://hydro1.gesdisc.eosdis.nasa.gov/data/GLDAS/GLDAS_NOAH025_3H.2.1/). The function uses varible name ('Tair_f_inst') for air temperature in GLDAS data products. Units for gridded air temperature data are degrees in 'K'. The GLDASpolyCentroid function outputs gridded air temperature (maximum and minimum) data in degrees 'C'.

    # The goal of the Global Land Data Assimilation System GLDAS is to ingest satellite and ground-based observational data products, using advanced land surface modeling and data assimilation techniques, in order to generate optimal fields of land surface states and fluxes (Rodell et al., 2004). GLDAS dataset used in this function is the GLDAS Noah Land Surface Model L4 3 hourly 0.25 x 0.25 degree V2.1. The full suite of GLDAS datasets is avaliable at https://hydro1.gesdisc.eosdis.nasa.gov/dods/. The GLDASpolyCentroid finds the minimum and maximum air temperatures for each day at each grid within the study watershed by searching for minima and maxima over the three hours air temperature data values available for each day and grid.

    # The GLDAS V2.1 simulation started on January 1, 2000 using the conditions from the GLDAS V2.0 simulation. The GLDAS V2.1 simulation was forced with National Oceanic and Atmospheric Administration NOAA, Global Data Assimilation System GDAS atmospheric analysis fields (Derber et al., 1991), the disaggregated Global Precipitation Climatology Project GPCP precipitation fields (Adler et al., 2003), and the Air Force Weather Agency’s AGRicultural METeorological modeling system AGRMET radiation fields which became available for March 1, 2001 onwards.

    #######Value

    # A table that includes points ID, Point file name, Lat, Long, and Elevation information formated to be read with SWAT, and a scalar of maximum and minimum air temperature gridded data values at each point within the study watershed in ascii format needed by SWAT model weather inputs will be stored at Dir.

    ######Note

    # start should be equal to or greater than 2000-Jan-01.

    ###Examples
    # GLDASpolyCentroid(Dir = "./SWAT_INPUT/", watershed = "LowerMekong.shp", DEM = "LowerMekong_dem.tif", start = "2015-12-1", end = "2015-12-3")
    logging.info("Running GLDASpolycentroid")
    url_GLDAS_input = 'https://hydro1.gesdisc.eosdis.nasa.gov/data/GLDAS/GLDAS_NOAH025_3H.2.1/'
    myvar = 'Tair_f_inst'
    start = datetime.datetime.strptime(start, '%Y-%m-%d').date()
    end = datetime.datetime.strptime(end, '%Y-%m-%d').date()
    ####Before getting to work on this function do this check
    if start >= datetime.date(2000, 1, 1):
        # SWAT climate 'precipitation' master file name
        filenametableKEY = Dir + 'temp_Master.txt'
        # Constructing time series based on start and end input days!
        time_period = pd.date_range(start, end).tolist()
        # Reading cell elevation data (DEM should be in geographic projection)
        watershed_elevation = georaster.SingleBandRaster(DEM, load_data=False)
        # Reading the study Watershed shapefile
        polys = gpd.read_file(watershed)
        subbasinCentroids = polys.geometry.centroid
        # Creation of a new resulting GeoDataFrame with the col, row and temperature values
        study_area_records = gpd.GeoDataFrame({'NAME': 'temp', 'geometry': subbasinCentroids}, crs=polys.crs)  #
        study_area_records['LONG'] = study_area_records.geometry.x
        study_area_records['LAT'] = study_area_records.geometry.y
        # lambda to evaluate elevation based on lat/long
        elev_x_y = lambda x, y: watershed_elevation.value_at_coords(x, y, latlon=True)
        study_area_records['ELEVATION'] = study_area_records.apply(lambda row: elev_x_y(row.LONG, row.LAT), axis=1)
        study_area_records = study_area_records.reset_index()
        study_area_records = study_area_records.rename(columns={'index': 'ID'})
        study_area_records['NAME'] = study_area_records['NAME'] + study_area_records['ID'].astype(str)
        #### Begin writing SWAT climate input tables
        #### Get the SWAT file names and then put the first record date
        if not os.path.exists(Dir):
            os.makedirs(Dir)
            os.chmod(os.path.join(Dir), 0o777)
        for h in range(study_area_records.shape[0]):
            filenameSWAT_TXT = Dir + study_area_records['NAME'][h] + '.txt'
            # write the data begining date once!
            swat = open(filenameSWAT_TXT, 'w')  #
            swat.write(format(time_period[0], '%Y%m%d'))
            swat.write('\n')
            swat.close()
        # Write out the SWAT grid information master table
        OutSWAT = pd.DataFrame(
            {'ID': study_area_records['ID'], 'NAME': study_area_records['NAME'], 'LAT': study_area_records['LAT'],
             'LONG': study_area_records['LONG'], 'ELEVATION': study_area_records['ELEVATION']})
        OutSWAT.to_csv(filenametableKEY, index=False)
        #### Start doing the work!
        #### iterate over days to extract records at GLDAS grids estabished in 'study_area_records'
        for kk in range(len(time_period)):
            julianDate = time_period[kk].strftime('%j')
            year = time_period[kk].strftime('%Y')
            myurl = url_GLDAS_input + year + '/' + julianDate + '/'
            check1 = requests.get(myurl)
            if check1.status_code == 200:
                filenames = check1._content
                # getting the subdaily files at each daily URL
                filenames = pd.DataFrame({'Web File': pd.read_html(filenames)[0][1]})
                filenames = filenames.dropna()
                warnings.filterwarnings("ignore", 'This pattern has match groups')
                criteria = filenames['Web File'].str.contains('GLDAS.+(.nc4$)')
                filenames = filenames[criteria]
                filenames = filenames.reset_index()
                SubdailyTemp = np.zeros([OutSWAT.shape[0], filenames.shape[0]])
                # creating array for min temp
                mindailytemp = np.zeros([polys.shape[0]])
                # creating array for max temp
                maxdailytemp = np.zeros([polys.shape[0]])
                # Extract the ncdf files within a day
                for gg in range(filenames.shape[0]):  # Iterating over each subdaily data file
                    subdailyfilename = filenames['Web File'][gg]
                    if not os.path.exists('./temp/'):
                        os.makedirs('./temp/')
                        os.chmod(os.path.join('./temp/'), 0o777)
                    destfile = './temp/' + subdailyfilename
                    subdailyfilename = myurl + subdailyfilename
                    r = requests.get(subdailyfilename, stream=True)
                    with open(destfile, 'wb') as fd:
                        os.chmod(os.path.join(destfile), 0o777)
                        fd.write(r.content)
                        fd.close()
                    # reading ncdf file
                    nc = netCDF4.Dataset(destfile)
                    data = nc.variables[myvar][0, :, :].data
                    # reorder the rows
                    data = np.flip(data, axis=0)
                    nodata = nc.missing_value
                    ind = np.where(data == nodata)
                    data[ind] = None
                    # calculate the Affine tranform and zonal weights once!
                    if gg == 0:
                        ###getting the y values (longitudes in degrees east)
                        nc_long = nc.variables['lon']
                        ####getting the x values (latitudes in degrees north)
                        nc_lat = nc.variables['lat']
                        ####getting the transform and resolutions for the IMERG raster data
                        xres = (nc_long[-1] - nc_long[0]) / nc_long.shape[0]
                        yres = (nc_lat[-1] - nc_lat[0]) / nc_lat.shape[0]
                        transform_GLDAS = rasterio.transform.from_origin(west=nc_long[0], north=nc_lat[-1], xsize=xres,
                                                                         ysize=yres)
                        ###calculating the weights for zonal average
                        weights = np.zeros(shape=(polys.shape[0], nc.dimensions['lat'].size, nc.dimensions['lon'].size))
                        myshape = (nc.dimensions['lat'].size, nc.dimensions['lon'].size)
                        for mm in range(polys.shape[0]):
                            geom = polys['geometry'][mm]
                            if geom.geometryType() == 'MultiPolygon':
                                geom = geom.convex_hull
                            weights[mm, :, :] = rasterize_pctcover(geom, transform_GLDAS, myshape) / 100
                    nc.close()
                    for ee in range(OutSWAT.shape[0]):
                        climate_values = weights[ee, :, :] * data
                        warnings.filterwarnings("ignore", 'invalid value encountered in double_scalars')
                        SubdailyTemp[ee, gg] = np.nansum(climate_values) / np.nansum(weights[ee, :, :])
                # obtain minimum daily data over the 3 hrs records
                warnings.filterwarnings("ignore", 'All-NaN slice encountered')
                mindailytemp = np.min(SubdailyTemp, axis=1)  # removing missing data
                mindailytemp = mindailytemp - 273.16  # convert to degree C
                mindailytemp[np.where(np.isnan(mindailytemp))] = -99  # filing missing data
                # same for maximum daily
                warnings.filterwarnings("ignore", 'All-NaN slice encountered')
                maxdailytemp = np.max(SubdailyTemp, axis=1)
                maxdailytemp = maxdailytemp - 273.16  # convert to degree C
                maxdailytemp[np.where(np.isnan(maxdailytemp))] = -99  # filing missing data
                # Looping through the GLDAS points and writing out the daily climate data in SWAT format
                for h in range(study_area_records.shape[0]):
                    filenameSWAT_TXT = Dir + study_area_records['NAME'][h] + '.txt'
                    # write the data begining date once!
                    with open(filenameSWAT_TXT, 'a') as swat:
                        temptext = str(maxdailytemp[h]) + ',' + str(mindailytemp[h])
                        swat.write(temptext)
                        swat.write('\n')
                        swat.close()
            shutil.rmtree('./temp/')

    else:
        print ('Sorry' + ", " + start.strftime("%b") + "-" + start.strftime(
            '%Y') + ' is out of coverage for GLDAS data products.')
        print ('Please pick start date equal to or greater than 2000-Jan-01 to access GLDAS data products.')
        print ('Thank you!')


def send_email(to_email, unique_id):

    from_email = 'nasaaccess@gmail.com'

    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Your nasaaccess data is ready'

    msg['From'] = from_email
    msg['To'] = to_email

    #email content
    message = """\
        <html>
            <head></head>
            <body>
                <p>Hello,
                   <br>
                   Your nasaaccess data is ready for download at 
                   <a href="http://tethys-servir.adpc.net/apps/nasaaccess2">
                        http://tethys-servir.adpc.net/apps/nasaaccess2
                   </a>
                   <br>
                   Your unique access code is: <strong>""" + unique_id + """</strong><br>
                </p>
            </body>
        <html>
    """

    part1 = MIMEText(message, 'html')
    msg.attach(part1)

    gmail_user = 'nasaaccess@gmail.com'
    gmail_pwd = 'nasaaccess123'
    smtpserver = smtplib.SMTP('smtp.gmail.com', 587)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo()
    smtpserver.login(gmail_user, gmail_pwd)
    smtpserver.sendmail(gmail_user, to_email, msg.as_string())
    smtpserver.close()

#  read in file paths and arguments from subprocess call in model.py
email = sys.argv[1]
functions = sys.argv[2].split(',')
unique_id = sys.argv[3]
shp_path = os.path.join(sys.argv[4])
dem_path = os.path.join(sys.argv[5])
unique_path = os.path.join(sys.argv[6],'')
tempdir = os.path.join(sys.argv[7],'')
start = sys.argv[8]
end = sys.argv[9]

os.makedirs(tempdir)
os.chmod(tempdir, 0o777)

os.makedirs(unique_path)
os.chmod(unique_path, 0o777)
unique_path = os.path.join(unique_path, 'nasaaccess_data')

os.makedirs(unique_path)
os.chmod(unique_path, 0o777)


# change working directory to temporary directory for storing intermediate data
os.chdir(tempdir)

#  Run nasaaccess functions requested by user
for func in functions:
    if func == 'GPMpolyCentroid':
        output_path = os.path.join(unique_path, 'GPMpolyCentroid', '')
        GPMpolyCentroid(output_path, shp_path, dem_path, start, end)
    elif func == 'GPMswat':
        output_path = os.path.join(unique_path, 'GPMswat', '')
        GPMswat(output_path, shp_path, dem_path, start, end)
    elif func == 'GLDASpolyCentroid':
        output_path = os.path.join(unique_path, 'GLDASpolyCentroid', '')
        GLDASpolyCentroid(output_path, shp_path, dem_path, start, end)
    elif func == 'GLDASwat':
        output_path = os.path.join(unique_path, 'GLDASwat', '')
        GLDASwat(output_path, shp_path, dem_path, start, end)

#  when data is ready, send the user an email with their unique access code
send_email(email, unique_id)

logging.info("Complete!!!")
import os, json
from .model import *
from django.http import JsonResponse, HttpResponseRedirect, HttpResponse
from django.core.files import File
from sqlalchemy.sql import text
from .app import swatdv
from .config import *

def get_upstream(request):
    """
    Controller to get list of all upstream reach ids and pass it to front end
    """
    watershed = request.POST.get('watershed')
    watershed_id = request.POST.get('watershed_id')
    streamID = request.POST.get('streamID')
    unique_id = request.POST.get('id')
    unique_path = os.path.join(temp_workspace, unique_id)
    if not os.path.exists(unique_path):
        os.makedirs(unique_path)
        os.chmod(unique_path, 0o777)

    upstreams = get_upstreams(watershed_id, streamID)

    json_dict = JsonResponse({'watershed': watershed, 'streamID': streamID, 'upstreams': upstreams})
    return json_dict

def save_json(request):
    """
    Controller to save upstream stream and subbasin json files to user's data cart
    """
    upstream_json = json.loads(request.body)
    bbox = upstream_json['bbox']
    srs = 'EPSG:'
    srs += upstream_json['crs']['properties']['name'].split(':')[-1]
    unique_id = upstream_json['uniqueId']
    outletID = upstream_json['outletID']
    feature_type = upstream_json['featureType']

    unique_path = os.path.join(temp_workspace, unique_id)
    with open(unique_path + '/' + feature_type + '_upstream_' + outletID + '.json', 'w') as outfile:
        json.dump(upstream_json, outfile)

    json_dict = JsonResponse({'id': unique_id, 'bbox': bbox, 'srs': srs})
    return json_dict

def clip_rasters(request):
    watershed = request.POST.get('watershed')
    userId = request.POST.get('userId')
    outletID = request.POST.get('outletID')
    raster_type = request.POST.get('raster_type')
    clip_raster(watershed, userId, outletID, raster_type)
    json_dict = JsonResponse({'watershed': watershed, 'raster_type': raster_type})
    return(json_dict)

def timeseries(request):
    """
    Controller for the time-series plot.
    """
    # Get values passed from the timeseries function in main.js
    watershed_id = int(request.POST.get('watershed_id'))
    watershed = request.POST.get('watershed')
    start = request.POST.get('startDate')
    end = request.POST.get('endDate')
    parameters = request.POST.getlist('parameters[]')
    streamID = request.POST.get('streamID')
    monthOrDay = request.POST.get('monthOrDay')
    file_type = request.POST.get('fileType')

    if file_type == 'rch':
        # Call the correct rch data parser function based on whether the monthly or daily toggle was selected
        if monthOrDay == 'Monthly':
            print({'Error': 'No monthly data available currently'})
            # timeseries_dict = extract_monthly_rch(watershed, start, end, parameters, streamID)
        else:
            timeseries_dict = extract_daily_rch(watershed, watershed_id, start, end, parameters, streamID)
    elif file_type == 'sub':
        timeseries_dict = extract_sub(watershed, watershed_id, start, end, parameters, streamID)

    # Return the json object back to main.js for timeseries plotting
    json_dict = JsonResponse(timeseries_dict)
    return json_dict

def coverage_compute(request):
    """
    Controller for computing the lulc or soil coverage statistics
    """
    uniqueID = request.POST.get('userID')
    outletID = str(request.POST.get('outletID'))
    watershed = request.POST.get('watershed')
    watershed_id = request.POST.get('watershed_id')
    raster_type = request.POST.get('raster_type')
    # clip_raster(watershed, uniqueID, outletID, raster_type)
    coverage_dict = coverage_stats(watershed, watershed_id, uniqueID, outletID, raster_type)
    json_dict = JsonResponse(coverage_dict)
    return(json_dict)

def run_nasaaccess(request):
    """
    Controller to call nasaaccess R functions.
    """
    # Get selected parameters and pass them into nasaccess R scripts
    userId = request.POST.get('userId')
    streamId = request.POST.get('streamId')
    start = request.POST.get('startDate')
    d_start = str(datetime.strptime(start, '%B %d, %Y').strftime('%Y-%m-%d'))
    end = request.POST.get(str('endDate'))
    d_end = str(datetime.strptime(end, '%B %d, %Y').strftime('%Y-%m-%d'))
    functions = request.POST.getlist('functions[]')
    watershed = request.POST.get('watershed')
    email = request.POST.get('email')
    nasaaccess_run(userId, streamId, email, functions, watershed, d_start, d_end)
    return HttpResponseRedirect('../')

def save_file(request):
    data_json = json.loads(request.body)
    file_dict = write_csv(data_json)
    json_dict = JsonResponse(file_dict)
    return json_dict

def download_files(request):
    if request.method == 'POST':
        uniqueID = request.POST['userID']

        data_dir = os.path.join(temp_workspace, uniqueID)

        zipfolder(data_dir, data_dir)

        path_to_file = os.path.join(temp_workspace, uniqueID + '.zip')
        f = open(path_to_file, 'r')
        myfile = File(f)

        response = HttpResponse(myfile, content_type='application/zip')
        response['Content-Disposition'] = 'attachment; filename=' + uniqueID + '.zip'
        return response

def update_selectors(request):
    watershed_id = request.POST.get('watershed_id')
    selector_dict = {'rch':{}, 'sub':{}, 'lulc':{}, 'soil':{}, 'stations':{}, 'nasaaccess':{}}
    Session = swatdv.get_persistent_store_database(db['name'], as_sessionmaker=True)
    session = Session()

    infqr = """SELECT sub,rch,lulc,soil,stations,nasaaccess FROM watershed_info WHERE watershed_id={0}""".format(watershed_id)
    infex = session.execute(text(infqr)).fetchall()

    sub_avail = infex[0][0]
    rch_avail = infex[0][1]
    lulc_avail = infex[0][2]
    soil_avail = infex[0][3]
    stat_avail = infex[0][4]
    nasaaccess_avail = infex[0][5]
    selector_dict['sub']['exists'] = sub_avail
    selector_dict['rch']['exists'] = rch_avail
    selector_dict['lulc']['exists'] = lulc_avail
    selector_dict['soil']['exists'] = soil_avail
    selector_dict['stations']['exists'] = stat_avail
    selector_dict['nasaaccess']['exists'] = nasaaccess_avail


    if rch_avail == 'Yes':
        dqrch = """SELECT rch_start,rch_end FROM watershed_info WHERE watershed_id={0}""".format(
            watershed_id)
        rchdex = session.execute(text(dqrch)).fetchall()
        rch_start = rchdex[0][0].strftime("%b %d, %Y")
        selector_dict['rch']['start'] = rch_start
        rch_end = rchdex[0][1].strftime("%b %d, %Y")
        selector_dict['rch']['end'] = rch_end

        vqrch = """SELECT rch_vars FROM watershed_info WHERE watershed_id={0}""".format(watershed_id)
        rchvex = session.execute(text(vqrch)).fetchall()
        rchvex = rchvex[0][0].split(',')
        rch_options = []
        for var in rchvex:
            option = (rch_param_names[var], var)
            rch_options.append(option)
        selector_dict['rch']['vars'] = rch_options

    if sub_avail == 'Yes':
        dqsub = """SELECT sub_start,sub_end FROM watershed_info WHERE watershed_id={0}""".format(
            watershed_id)
        subdex = session.execute(text(dqsub)).fetchall()
        sub_start = subdex[0][0].strftime("%b %d, %Y")
        selector_dict['sub']['start'] = sub_start
        sub_end = subdex[0][1].strftime("%b %d, %Y")
        selector_dict['sub']['end'] = sub_end

        vqsub = """SELECT sub_vars FROM watershed_info WHERE watershed_id={0}""".format(watershed_id)
        subvex = session.execute(text(vqsub)).fetchall()
        subvex = subvex[0][0].split(',')
        sub_options = []
        for var in subvex:
            option = (sub_param_names[str(var)], str(var))
            sub_options.append(option)
        selector_dict['sub']['vars'] = sub_options

    session.close()
    json_dict = JsonResponse(selector_dict)
    return json_dict
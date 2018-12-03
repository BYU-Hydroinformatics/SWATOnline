import os
from .app import swatdv

temp_workspace = os.path.join(swatdv.get_app_workspace().path, 'swat')

data_path = os.path.join('/home/ubuntu/swat_data/')

gdalwarp_path = os.path.join('/home/ubuntu/tethys/miniconda/envs/tethys/bin/gdalwarp')

geoserver = {'rest_url':'http://216.218.240.206:8080/geoserver/rest/',
             'wms_url':'http://216.218.240.206:8080/geoserver/wms/',
             'user':'admin',
             'password':'geoserver',
             'workspace':'swat'
             }

db = {'name': 'swat_db', 'user':'tethys_super', 'pass':'pass', 'host':'localhost', 'port':'5435'}

nasaaccess_path = os.path.join('/home/ubuntu/nasaaccess_data')

nasaaccess_temp = os.path.join(swatdv.get_app_workspace().path, 'nasaaccess')

nasaaccess_py3 = os.path.join('/home/ubuntu/tethys/miniconda/envs/nasaaccess/bin/python3')

nasaaccess_script = os.path.join('/home/ubuntu/subprocesses/nasaaccess.py')

nasaaccess_log = os.path.join('/home/ubuntu/subprocesses/nasaaccess.log')
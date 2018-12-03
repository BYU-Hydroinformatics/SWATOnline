import os
import sys
from setuptools import setup, find_packages
from tethys_apps.app_installation import custom_develop_command, custom_install_command

### Apps Definition ###
app_package = 'nasaaccess'
release_package = 'tethysapp-' + app_package
app_class = 'nasaaccess.app:Nasaaccess'
app_package_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tethysapp', app_package)

### Python Dependencies ###
dependencies = []

setup(
    name=release_package,
    version='0.0.1',
    tags='&quot;Hydrology&quot;, &quot;GLDAS&quot;, &quot;GPM&quot;, &quot;SWAT&quot;',
    description='Web interface for downloading precipitation and air temperature data from NASA&#39;s EarthData website',
    long_description='',
    keywords='',
    author='Spencer McDonald',
    author_email='spencermcdonald11@gmail.com',
    url='',
    license='',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['tethysapp', 'tethysapp.' + app_package],
    include_package_data=True,
    zip_safe=False,
    install_requires=dependencies,
    cmdclass={
        'install': custom_install_command(app_package, app_package_dir, dependencies),
        'develop': custom_develop_command(app_package, app_package_dir, dependencies)
    }
)

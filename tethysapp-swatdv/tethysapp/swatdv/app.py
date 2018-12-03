from tethys_sdk.base import TethysAppBase, url_map_maker
from tethys_sdk.app_settings import PersistentStoreDatabaseSetting

class swatdv(TethysAppBase):
    """
    Tethys app class for SWAT Data Viewer.
    """

    name = 'SWAT Data Viewer'
    index = 'swatdv:home'
    icon = 'swatdv/images/logo.png'
    package = 'swatdv'
    root_url = 'swatdv'
    color = '#2d2d2d'
    description = 'Application to access and analyse the inputs and outputs of the Soil and Water Assessment Tool (SWAT)'
    tags = '&quot;Hydrology&quot;, &quot;Soil&quot;, &quot;Water&quot;, &quot;Timeseries&quot;'
    enable_feedback = False
    feedback_emails = []

    def url_maps(self):
        """
        Add controllers
        """
        UrlMap = url_map_maker(self.root_url)

        url_maps = (
            UrlMap(
                name='home',
                url='swatdv',
                controller='swatdv.controllers.home'
            ),
            UrlMap(
                name='update_selectors',
                url='swatdv/update_selectors',
                controller='swatdv.ajax_controllers.update_selectors'
            ),
            UrlMap(
                name='get_upstream',
                url='swatdv/get_upstream',
                controller='swatdv.ajax_controllers.get_upstream'
            ),
            UrlMap(
                name='save_json',
                url='swatdv/save_json',
                controller='swatdv.ajax_controllers.save_json'
            ),
            UrlMap(
                name='timeseries',
                url='swatdv/timeseries',
                controller='swatdv.ajax_controllers.timeseries'
            ),
            UrlMap(
                name='clip_rasters',
                url='swatdv/clip_rasters',
                controller='swatdv.ajax_controllers.clip_rasters'
            ),
            UrlMap(
                name='coverage_compute',
                url='swatdv/coverage_compute',
                controller='swatdv.ajax_controllers.coverage_compute'
            ),
            UrlMap(
                name='run_nasaaccess',
                url='swatdv/run_nasaaccess',
                controller='swatdv.ajax_controllers.run_nasaaccess'
            ),
            UrlMap(
                name='save_file',
                url='swatdv/save_file',
                controller='swatdv.ajax_controllers.save_file'
            ),
            UrlMap(
                name='download_files',
                url='swatdv/download_files',
                controller='swatdv.ajax_controllers.download_files'
            ),
        )

        return url_maps

    def persistent_store_settings(self):
        ps_settings = (
            PersistentStoreDatabaseSetting(
                name='swat_db',
                description='Primary database for SWAT Online app.',
                initializer='swatdv.model.init_db',
                required=True
            ),
        )

        return ps_settings

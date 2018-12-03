from django.shortcuts import *
from tethys_sdk.gizmos import *
from datetime import datetime
from .app import swatdv
from .model import *
from .config import *
from sqlalchemy.sql import text

def home(request):
    """
    Controller for the Output Viewer page.
    """
    # Get available watersheds and set select_watershed options
    Session = swatdv.get_persistent_store_database(db['name'], as_sessionmaker=True)
    session = Session()
    # Query DB for regions
    wqr = """SELECT * FROM watershed"""
    watersheds = session.execute(text(wqr)).fetchall()

    watershed_options = []

    for f in watersheds:
        name = f.name
        name = name.replace('_', ' ').title()
        value = f.name
        id = f.id
        val = str(id)+'|'+str(value)
        watershed_options.append((name,val))


    watershed_select = SelectInput(name='watershed_select',
                                   multiple=False,
                                   original=False,
                                   options=watershed_options,
                                   initial=[('Lower Mekong', 'lower_mekong')],
                                   )


    na_start = 'Jan 01, 2000'
    na_end = datetime.now().strftime("%b %d, %Y")
    na_format = 'MM d, yyyy'
    na_startView = 'decade'
    na_minView = 'days'


    rch_start_pick = DatePicker(name='rch_start_pick',
                            autoclose=True,
                            today_button=False,
                            initial='Start Date',
                            classes = 'rch_date start_date'
                            )

    rch_end_pick = DatePicker(name='rch_end_pick',
                          autoclose=True,
                          today_button=False,
                          initial='End Date',
                          classes = 'rch_date end_date'
                          )

    sub_start_pick = DatePicker(name='sub_start_pick',
                                autoclose=True,
                                today_button=False,
                                initial='Start Date',
                                classes='sub_date start_date'
                                )

    sub_end_pick = DatePicker(name='sub_end_pick',
                              autoclose=True,
                              today_button=False,
                              initial='End Date',
                              classes='sub_date end_date'
                              )

    na_start_pick = DatePicker(name='na_start_pick',
                            autoclose=True,
                            format=na_format,
                            min_view_mode=na_minView,
                            start_date=na_start,
                            end_date=na_end,
                            start_view=na_startView,
                            today_button=False,
                            initial='Start Date')

    na_end_pick = DatePicker(name='na_end_pick',
                          autoclose=True,
                          format=na_format,
                          min_view_mode=na_minView,
                          start_date=na_start,
                          end_date=na_end,
                          start_view=na_startView,
                          today_button=False,
                          initial='End Date'
                          )

    rch_var_select = SelectInput(name='rch_var_select',
                               multiple=True,
                               original=False,
                               classes='rch_var',
                               select2_options={'placeholder': 'Select Variable(s)',
                                                'allowClear': False},

                               )

    sub_var_select = SelectInput(name='sub_var_select',
                                 multiple=True,
                                 original=False,
                                 classes='sub_var',
                                 select2_options={'placeholder': 'Select Variable(s)',
                                                  'allowClear': False},
                                 )

    session.close()

    context = {
        'rch_start_pick': rch_start_pick,
        'rch_end_pick': rch_end_pick,
        'na_start_pick': na_start_pick,
        'na_end_pick': na_end_pick,
        'sub_start_pick': sub_start_pick,
        'sub_end_pick': sub_end_pick,
        'rch_var_select': rch_var_select,
        'sub_var_select': sub_var_select,
        'watershed_select': watershed_select,
    }

    return render(request, 'swatdv/home.html', context)
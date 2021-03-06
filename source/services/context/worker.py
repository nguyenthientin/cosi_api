import sqlite3
import logging
import traceback
import pprint
import json
import numpy as np
from logging.handlers import QueueHandler
from schema import Schema, And, Or, Use, Optional, SchemaError
from datetime import datetime, timedelta
from copy import deepcopy

from services.common.database import db_select
from services.common.parameters import date_check, date_convert, db_fields_check, range_check
from services.common.utils import array2rgb

import pickle

def run(request, queue, log_queue = None):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if log_queue:
        handler = QueueHandler(log_queue)
    else:
        # no logging queue, so simply disable logging
        handler = logging.NullHandler()
    logger.addHandler(handler)

    logger.info("Started context search")

    try:
        request = validate_schema(request)
    except SchemaError as exc:
        message = {"error": repr(exc)}
        print(message)
        logger.error("Error in schema validation, see result for details")
        queue.put(message)
        return
    logger.info("Request schema validated")

    try:
        return_fields = deepcopy(request['return'])
        # distance: convert km to m
        if 'space_ext' in request:
            request['space_ext'] = [x * 1000 for x in request['space_ext']]
        conditions = []
        if 'date' in request:
            fmt = '%Y-%m-%d'
            if request['date']['type'] == 'individual':
                cond = 'date IN ('
                for v in request['date']['value']:
                    cond += '\' {} \', '.format(v.strftime(fmt))
                cond = cond[:-2] + ')'
            else:
                cond = 'date between \'{}\' and \'{}\''.format(
                    request['date']['value'][0].strftime(fmt),
                    request['date']['value'][1].strftime(fmt)
                )
            conditions.append(cond)
        if 'road_num' in request:
            cond = ''
            for rn in request['road_num']:
                rn_str = '{:03d}'.format(rn)
                if request['database']=='debug':
                    cond += 'road_num like \'%{}%\' AND '.format(rn_str)
                else:
                    cond += 'road_number like \'%{}%\' AND '.format(rn_str)
            cond = cond[:-5]
            conditions.append(cond)
        if 'time_ext' in request:
            cond = 'time_extent between {} and {}'.format(*request['time_ext'])
            conditions.append(cond)
        if 'space_ext' in request:
            cond = 'space_extent between {} and {}'.format(*request['space_ext'])
            conditions.append(cond)
        if 'total_delay' in request:
            cond = 'total_delay between {} and {}'.format(*request['total_delay'])
            conditions.append(cond)
        if 'number_of_disturbances' in request:
            cond = 'number_of_disturbances between {} and {}'.format(*request['number_of_disturbances'])
            conditions.append(cond)
        if 'speed' in request['return'] or 'flow' in request['return']:
            if 'space_resolution' not in request['return']:
                request['return'].append('space_resolution')
            if 'time_resolution' not in request['return']:
                request['return'].append('time_resolution')
            if 'time' not in request['return']:
                request['return'].append('time')
            if 'date' not in request['return']:
                request['return'].append('date')
        data = db_select(request['return'], conditions,
                         num=request['num_pattern'],
                         debug=request['database']=='debug')
        if 'date' in request['return']:
            for p in data.values():
                p['date'] = p['date'].strftime('%Y-%m-%d')
        if request['database'] == 'cosi':
            if 'speed' in request['return'] or 'flow' in request['return']:
                for p in data.values():
                    tstart = p['time'].lower
                    tend = p['time'].upper
                    t_list = []
                    dt = p['time_resolution']
                    for s in range(0,int((tend - tstart).seconds)+dt,dt):
                        t = tstart + timedelta(seconds=s)
                        t_list.append(t.strftime('%H:%M:%S'))
                    p.pop('time', None)
                    p['t'] = t_list
                    dx = p['space_resolution']
                    x_dim = np.array(p['speed']).shape[0]
                    x = np.arange(0,x_dim)*dx
                    p['x'] = x.tolist()
                           
#         data[0]['time'] = [data[0]['time'].lower, data[0]['time'].upper]
#         print(type(t0))
        # convert speed/flow to images
        if data:
            available_fields = data[0].keys()
            removed_fields = [f for f in available_fields if f not in return_fields]
            print(return_fields)
            print(removed_fields)
            for p in data.values():
                for field in removed_fields:
                    p.pop(field, None)
        if request['convert_image']:
            for p in data.values():
                if 'speed' in p:
                    speed = np.array(p['speed'])
#                     speed = np.array(json.loads(p['speed']))
                    rgb = array2rgb(speed, request['cmap'], num_level=256)
                    p['speed_rgb'] = json.dumps(rgb.tolist())
                if 'flow' in p:
                    flow = np.array(p['flow'])
#                     flow = np.array(json.loads(p['flow']))
                    rgb = array2rgb(flow,request['cmap'],num_level=2500)
                    p['flow_rgb'] = json.dumps(rgb.tolist())
        if not request['return_speed']:
            for p in data.values():
                p.pop('speed', None)
        logger.info("completed successfully")
#         print(data)
        queue.put(data)
    # pylint: disable=broad-except
    except Exception as exc:
        message = {"error": repr(exc)}
        print(message)
        logger.error("Error encountered")
        logger.error(traceback.format_exc())
        queue.put(message)

def get_schema():
    date_validate = And(date_check, Use(date_convert))
    number = Or(float, int)
    context_search_schema = Schema({
        "service": "context_search",
        Optional('return', default=['id', 'speed', 'date', 'road_number', 'space_extent', 'time_extent', 'number_of_disturbances', 'total_delay']): db_fields_check,
        Optional("date"): Or(
            {'type': 'range',
             'value': And([date_validate, date_validate], range_check)},
            {'type': 'individual',
             'value': [date_validate,]}
        ),
        Optional('road_num'): [int,],
        Optional('time_ext'): And([number, number], range_check),
        Optional('space_ext'): And([number, number], range_check),
        Optional('total_delay'): And([number, number], range_check),
        Optional('number_of_disturbances'): And([int, int], range_check),
        Optional('num_pattern', default=10): int,
        Optional('convert_image', default=False): bool,
        Optional('return_speed', default=True): bool,
        Optional('cmap', default='RdYlGn'): str,
        Optional('database', default='cosi'): str
    }, ignore_extra_keys=True)
    return context_search_schema

def validate_schema(request):
    context_search_schema = get_schema()
    return context_search_schema.validate(request)

def info():
    """
    return information about the service
    """
    context_search_schema = get_schema()

    # pylint: disable=import-outside-toplevel
    # from tests.context import TEST_DATA
    # reference_request = TEST_DATA / "ndwQuery.json"
    # with open(reference_request) as reference_file:
    #     example = reference_file.read()

    info_text = """
Information for search-by-context service

Input units:
* "date": date string in the format "YYYY-MM-DD"
* "space_ext": 100 meters
* "time_ext": minutes
"""

    info_text += "\n"
    info_text += "Schema:\n"
    info_text += pprint.pformat(context_search_schema.schema, width=1, indent=3)

    # info_text += "\n"*2 + "-"*50 + "\n"*2
    # info_text += "Example:\n"
    # info_text += example

    return info_text

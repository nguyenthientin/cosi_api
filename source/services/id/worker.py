import sqlite3
import logging
import traceback
import pprint
import json
import numpy as np
from logging.handlers import QueueHandler
from schema import Schema, And, Or, Use, Optional, SchemaError
from datetime import datetime, timedelta

from services.common.database import db_select
from services.common.parameters import date_check, date_convert, db_fields_check, range_check

def run(request, queue, log_queue = None):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if log_queue:
        handler = QueueHandler(log_queue)
    else:
        # no logging queue, so simply disable logging
        handler = logging.NullHandler()
    logger.addHandler(handler)

    logger.info("Started data retrieval")

    try:
        request = validate_schema(request)
    except SchemaError as exc:
        message = {"error": repr(exc)}
        logger.error("Error in schema validation, see result for details")
        queue.put(message)
        return
    logger.info("Request schema validated")

    try:
        conditions = ['id = {}'.format(request['id'])]
        if 'speed' in request['return'] or 'flow' in request['return']:
            if 'space_resolution' not in request['return']:
                request['return'].append('space_resolution')
            if 'time_resolution' not in request['return']:
                request['return'].append('time_resolution')
        data = db_select(request['return'], conditions,
                         num=1,
                         debug=False)
        if 'speed' in request['return'] or 'flow' in request['return']:
            for p in data.values():
                tstart = p['time'].lower
                tend = p['time'].upper
                t_str_list = []
                min_list = []
                dt = p['time_resolution']
                for s in range(0,int((tend - tstart).seconds)+dt,dt):
                    t = tstart + timedelta(seconds=s)
                    t_str_list.append(t.strftime('%H:%M:%S'))
                    min_list.append(t.hour*60 + t.minute + t.second/60)
                p.pop('time', None)
                p['t_tt'] = min_list
                p['t'] = t_str_list
                dx = p['space_resolution']
                x_dim = np.array(p['speed']).shape[0]
                x = np.arange(0,x_dim)*dx
                p['x'] = x.tolist()
        logger.info("completed successfully")
        if data:
            data = data[0]
        queue.put(data)
    # pylint: disable=broad-except
    except Exception as exc:
        message = {"error": repr(exc)}
        logger.error("Error encountered")
        logger.error(traceback.format_exc())
        queue.put(message)

def get_schema():
    key_search_schema = Schema({
        "service": "data_retrieval",
        "id": int,
        Optional('return', default=['id', 'speed', 'flow', 'linestring', 'date', 'time']): db_fields_check,
    }, ignore_extra_keys=True)
    return key_search_schema

def validate_schema(request):
    key_search_schema = get_schema()
    return key_search_schema.validate(request)

def info():
    """
    return information about the service
    """
    key_search_schema = get_schema()

    # pylint: disable=import-outside-toplevel
    # from tests.context import TEST_DATA
    # reference_request = TEST_DATA / "ndwQuery.json"
    # with open(reference_request) as reference_file:
    #     example = reference_file.read()

    info_text = """
Information for retrieve-by-key service
"""

    info_text += "\n"
    info_text += "Schema:\n"
    info_text += pprint.pformat(key_search_schema.schema, width=1, indent=3)

    # info_text += "\n"*2 + "-"*50 + "\n"*2
    # info_text += "Example:\n"
    # info_text += example

    return info_text
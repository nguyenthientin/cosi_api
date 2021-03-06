import sqlite3
import logging
import traceback
import pprint
import json
import numpy as np
from logging.handlers import QueueHandler
from schema import Schema, And, Or, Use, Optional, SchemaError

from services.parse_input_string.parsers import parse_dates, parse_time, parse_distance, parse_road, parse_number_of_disturbances, parse_total_delay

def run(request, queue, log_queue = None):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    if log_queue:
        handler = QueueHandler(log_queue)
    else:
        # no logging queue, so simply disable logging
        handler = logging.NullHandler()
    logger.addHandler(handler)

    logger.info("Started parse input service")

    try:
        valid_request = validate_schema(request)
        request = valid_request
    except SchemaError as exc:
        message = {"error": repr(exc)}
        print('Error in schema validation')
        print(request)
        print(message)
        logger.error("Error in schema validation, see result for details")
        queue.put(message)
        return
    logger.info("Request schema validated")

    try:
        txt = request['input_str'].replace(' ', '')
        groups_str = txt.split('+')
        data = {'date': [], 'time_ext': [], 'space_ext': [], 'road_num': [], 'number_of_disturbances': [], 'total_delay': [], 'warning': []}
        
        for _g in groups_str:
            try:
                d = parse_dates(_g)
                data['date'].append(d)
                continue
            except Exception:
                pass
            try:
                t = parse_time(_g)
                data['time_ext'] = t
                continue
            except Exception:
                pass
            try:
                d = parse_distance(_g)
                data['space_ext'] = d
                continue
            except Exception:
                pass
            try:
                r = parse_road(_g)
                data['road_num'].append(r)
                continue
            except Exception:
                pass
            try:
                n = parse_number_of_disturbances(_g)
                data['number_of_disturbances'] = n
                continue
            except Exception:
                pass
            try:
                n = parse_total_delay(_g)
                data['total_delay'] = n
                continue
            except Exception:
                pass
            # not one of the above
            data['warning'].append(_g)
        for k,v in data.items():
            if k != 'road_num' and len(v) == 1:
                data[k] = v[0]
        # displayed text
        text = ''
        if data['road_num'] != []:
            roads = ''
            for r in data['road_num']:
                roads += 'A{}, '.format(r)
            text += 'Wegnummer: {} | '.format(roads[:-2])
#             text += 'Wegnummer: A{} | '.format(data['road_num'])
        if data['date'] != []:
            dates = ''
            if data['date']['type'] == 'range':
                dates += '['
            for d in data['date']['value']:
                dates += '{}, '.format(d)
            dates = dates[:-2]
            if data['date']['type'] == 'range':
                dates += ']'
            text += 'Datum: {} | '.format(dates)
        if data['space_ext'] != []:
            text += 'File lengte: {}-{} km | '.format(data['space_ext'][0], data['space_ext'][1])
        if data['time_ext'] != []:
            text += 'File duur: {}-{} mins | '.format(data['time_ext'][0], data['time_ext'][1])
        if data['number_of_disturbances'] != []:
            text += 'Aantal filegolven: {}-{} | '.format(data['number_of_disturbances'][0],data['number_of_disturbances'][1])
        if data['total_delay'] != []:
            text += 'Total delay: {}-{} | '.format(data['total_delay'][0],data['total_delay'][1])
        text = text[:-3]
        data['text'] = text.replace(' ', '_')
        logger.info("completed successfully")
        print(data)
        queue.put(data)
    # pylint: disable=broad-except
    except Exception as exc:
        message = {"error": repr(exc)}
        print(message)
        logger.error("Error encountered")
        logger.error(traceback.format_exc())
        queue.put(message)

def get_schema():
    input_str_schema = Schema({
        "service": "parse_input",
        "input_str": str
    }, ignore_extra_keys=True)
    return input_str_schema

def validate_schema(request):
    input_str_schema = get_schema()
    return input_str_schema.validate(request)

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

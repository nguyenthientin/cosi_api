from datetime import datetime
from services.common.parameters import date_convert
import re

def parse_dates(datestr):
    # check if this is date range
    fmt = re.compile('(.*)-(.*)')
    res = re.match(fmt, datestr)
    if res is None:
        d = date_convert(datestr)
        return {'type': 'individual', 'value': [d.strftime('%Y-%m-%d')]}
    try:
        from_date = date_convert(res.group(1))
        to_date = date_convert(res.group(2))
        return {'type': 'range', 'value': [from_date.strftime('%Y-%m-%d'),to_date.strftime('%Y-%m-%d')]}
    except Exception:
        raise ValueError('not date/date range format')
def parse_distance(distance):
    arange = parse_range(distance)
    if not arange:
        distance = '0-' + distance
    fmt = re.compile('([0-9]+(?:\.[0-9]+)?)-([0-9]+(?:\.[0-9]+)?)(?=km)')
    res = re.match(fmt, distance)
    if res is not None:
        distance_lower = float(res.group(1))
        distance_upper = float(res.group(2))
        return [distance_lower, distance_upper]
    else:
        raise ValueError('not distance range format')
def parse_time(time):
    arange = parse_range(time)
    if not arange:
        time = '0-' + time
    fmt = re.compile('([0-9]+(?:\.[0-9]+)?)-([0-9]+(?:\.[0-9]+)?)(?=min)')
    res = re.match(fmt, time)
    if res is not None:
        time_from = float(res.group(1))
        time_to = float(res.group(2))
        return [time_from, time_to]
    else:
        raise ValueError('not time range format')
def parse_road(road):
    fmt = re.compile('[a,A,n,N,s,S]([0-9]+)')
    res = re.match(fmt, road)
    if res is not None:
        return int(res.group(1))
    else:
        raise ValueError('not road number')
def parse_number_of_disturbances(disturb):
    if '#' not in disturb:
        raise ValueError('not disturbance-number format')
    disturb = disturb.replace('#', '')
    arange = parse_range(disturb)
    if not arange:
        disturb = '0-' + disturb
    fmt = re.compile('([0-9]+(?:\.[0-9]+)?)-([0-9]+(?:\.[0-9]+)?)')
    res = re.match(fmt, disturb)
    if res is not None:
        return [int(res.group(1)), int(res.group(2))]
    else:
        raise ValueError('not disturbance-number format')
def parse_total_delay(totaldelay):
    if 'vvu' not in totaldelay:
        raise ValueError('not total delay format')
    totaldelay = totaldelay.replace('vvu', '')
    arange = parse_range(totaldelay)
    if not arange:
        totaldelay = '0-' + totaldelay
    fmt = re.compile('([0-9]+(?:\.[0-9]+)?)-([0-9]+(?:\.[0-9]+)?)')
    res = re.match(fmt, totaldelay)
    if res is not None:
        return [int(res.group(1)), int(res.group(2))]
    else:
        raise ValueError('not total delay format')
def parse_range(string):
    fmt = re.compile('(.*)-(.*)')
    res = re.match(fmt, string)
    if res is None:
        return None
    return [res.group(1), res.group(2)]
    
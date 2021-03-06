import sqlite3
import services
import psycopg2
import json
from services.credentials import database_settings

def db_select(fields, conditions, num=10, debug=True):
    if debug:
        db_settings = database_settings['debug']
        with sqlite3.connect(db_settings['file_path']) as conn:
            cursor = conn.cursor()
            data = _db_select(cursor, db_settings['table'], fields, conditions, num)
            return data
    else:
        for i,f in enumerate(fields):
            if f == 'road_num':
                fields[i] = 'road_number'
        db_settings = database_settings['cosi']
        print(db_settings)
        connect_kwargs = db_settings['connect_args']
        print(connect_kwargs)
        with psycopg2.connect(**connect_kwargs) as conn:
            cursor = conn.cursor()
            data = _db_select_cosi(cursor, db_settings['table'], fields, conditions, num)
            return data

def _db_select_cosi(cursor, table_name, fields, conditions, num):
    # what fields to retrieve
    fields_str = ''
    for fld in fields:
        fields_str += fld + ', '
    # remove the last comma (and the space afterwards)
    fields_str = fields_str[:-2]
    print(conditions)
    # conditions
    conditions_str = ''
    for cond in conditions:
        conditions_str += cond + " AND "
    # remove the last AND 
    conditions_str = conditions_str[:-5]

    num = min(num, services.max_retrieve_pattern)

    print('SELECT {} FROM {} WHERE {} LIMIT {}'.format(
        fields_str, table_name, conditions_str,
        num
    ))
    cursor.execute('SELECT {} FROM {} WHERE {} ORDER BY total_delay DESC LIMIT {}'.format(
        fields_str, table_name, conditions_str,
        num
    ))
    res = cursor.fetchall()
    if res != []:
        data = {}
        for i,p in enumerate(res):
            p_dict = {}
            for j,fld in enumerate(fields):
                p_dict[fld] = p[j]
            data[i] = p_dict
        for p in data.values():
            if 'road_number' in p:
                rn = []
                for r in json.loads(p['road_number']):
                    if r.isdigit():
                        rn.append('A{}'.format(int(r)))
                    else:
                        rn.append(r)
                p['road_number'] = json.dumps(rn)
            if 'linestring' in p:
                lnstr = json.loads(p['linestring'])
                for i,c in enumerate(lnstr['geometry']['coordinates']):
                    lnstr['geometry']['coordinates'][i] = [c['lng'], c['lat']]
                p['linestring'] = json.dumps(lnstr)
            if 'speed' in p:
                p['speed'] = json.loads(p['speed'])
            if 'flow' in p:
                p['flow'] = json.loads(p['flow'])
            if 'space_extent' in p:
                p['space_extent'] = int(p['space_extent'] / 1000)
        return data
    else:
        return {}

def _db_select(cursor, table_name, fields, conditions, num):
    # what fields to retrieve
    fields_str = ''
    for fld in fields:
        fields_str += fld + ', '
    # remove the last comma (and the space afterwards)
    fields_str = fields_str[:-2]
    # conditions
    conditions_str = ''
    for cond in conditions:
        conditions_str += cond + " AND "
    # remove the last AND 
    conditions_str = conditions_str[:-5]

    num = min(num, services.max_retrieve_pattern)

    print('SELECT {} FROM {} WHERE {} LIMIT {}'.format(
        fields_str, table_name, conditions_str,
        num
    ))
    cursor.execute('SELECT {} FROM {} WHERE {} LIMIT {}'.format(
        fields_str, table_name, conditions_str,
        num
    ))
    res = cursor.fetchall()
    if res != []:
        data = {}
        for i,p in enumerate(res):
            p_dict = {}
            for j,fld in enumerate(fields):
                p_dict[fld] = p[j]
            data[i] = p_dict
        return data
    else:
        return {}

def db_select2(fields, conditions, num=10):
    with sqlite3.connect(database_settings['file_path']) as conn:
        cursor = conn.cursor()
        # what fields to retrieve
        fields_str = ''
        for fld in fields:
            fields_str += fld + ', '
        # remove the last comma (and the space afterwards)
        fields_str = fields_str[:-2]
        # conditions
        conditions_str = ''
        for cond in conditions:
            conditions_str += cond + " AND "
        # remove the last AND 
        conditions_str = conditions_str[:-5]

        num = min(num, services.max_retrieve_pattern)

        print('SELECT {} FROM {} WHERE {} LIMIT {}'.format(
            fields_str, database_settings['table'], conditions_str,
            num
        ))
        cursor.execute('SELECT {} FROM {} WHERE {} LIMIT {}'.format(
            fields_str, database_settings['table'], conditions_str,
            num
        ))
        res = cursor.fetchall()
        if res != []:
            data = {}
            for i,p in enumerate(res):
                p_dict = {}
                for j,fld in enumerate(fields):
                    p_dict[fld] = p[j]
                data[i] = p_dict
            return data
        else:
            return {}

def pattern_by_id(key, *args):
    with sqlite3.connect(database_settings['file_path']) as conn:
        cursor = conn.cursor()
        # what fields to retrieve
        fields = ''
        for name in args:
            fields += name + ', '
        # remove the last comma (and the space afterwards)
        fields = fields[:-2]
        cursor.execute('SELECT {} from {} where id={}'.format(
            fields, database_settings['table'], key
        ))
        res = cursor.fetchall()
        if res != []:
            data = {}
            for i,name in enumerate(args):
                data[name] = res[i]
            return data
        else:
            return None
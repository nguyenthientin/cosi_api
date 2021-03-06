from datetime import datetime

def date_check(datestr):
    try:
        date_convert(datestr)
        return True
    except ValueError:
        return False
def date_convert(datestr):
    possible_sep = [' ', '/', '-', ',']
    for s in possible_sep:
        datestr = datestr.replace(s, '')
    possible_fmt = ['%Y%m%d',
                    '%d%m%Y',
                    '%d%m%y',
                    '%d%b%Y',
                    '%d%B%Y']
    for fmt in possible_fmt:
        try:
            d = datetime.strptime(datestr, fmt)
            return d
        except Exception:
            continue
    raise ValueError('date format is not recognised')

def db_fields_check(fields):
    return True

def range_check(arange):
    try:
        range_get(arange)
        return True
    except ValueError:
        return False
def range_get(arange):
    assert len(arange) == 2, '2 values needed for a range'
    return arange
    
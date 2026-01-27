import datetime as dt
from dateutil.relativedelta import relativedelta


def setLevelDate(_lvl = 1):
    if (_lvl == 1):
        return {'months': 6}
    elif (_lvl == 2):
        return {'months': 3}
    elif (_lvl == 3):
        return {'months': 1}
    elif (_lvl == 4):
        return {'days': 1}
    elif (_lvl == 5):
        return {'hours': 1}
    elif (_lvl == 6):
        return {'minutes': 10}
    elif (_lvl == 7):
        return {'minutes': 5}
    elif (_lvl == 8):
        return {'minutes': 1}
    else:
        print('Error! Try value in range 1...8')
        return False
    

def GetDateTimeIntervales(
        date_from: str,
        date_to: str = dt.datetime.now().strftime(r"%Y-%m-%dT%H:%M:%S.00+03:00"),
        interval_lvl: int = 1
    ):
    output = []

    start_date  = dt.datetime.fromisoformat(date_from)
    end_date    = dt.datetime.fromisoformat(date_to)

    cur_from    = start_date

    while (cur_from < end_date):
        element = {
            'date_from':    None,
            'date_to':      None
        }

        if any(key in setLevelDate(interval_lvl) for key in ['years', 'months']):
            cur_to = cur_from + relativedelta(**setLevelDate(interval_lvl))
        else:
            cur_to = cur_from + dt.timedelta(**setLevelDate(interval_lvl))


        if cur_to > end_date:
            cur_to = end_date
        
        element['date_from']    = cur_from.isoformat()
        element['date_to']      = cur_to.isoformat()

        output.append(element)

        cur_from = cur_to

    return output
import datetime as dt
from dateutil.relativedelta import relativedelta

def GetConfigurations():
    ...

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

def GetDateTimeIntervales(date_from: str, date_to: str, interval: dict):
    output = []

    start_date  = dt.datetime.fromisoformat(date_from)
    end_date    = dt.datetime.fromisoformat(date_to)

    cur_from    = start_date

    while (cur_from < end_date):
        element = {
            'date_from':    None,
            'date_to':      None
        }

        if any(key in interval for key in ['years', 'months']):
            cur_to = cur_from + relativedelta(**interval)
        else:
            cur_to = cur_from + dt.timedelta(**interval)


        if cur_to > end_date:
            cur_to = end_date
        
        element['date_from']    = cur_from.isoformat()
        element['date_to']      = cur_to.isoformat()

        output.append(element)

        cur_from = cur_to

    return output


def GetRegionsWithError():
    """
    Расширенная версия с поддержкой месяцев, недель и часовых поясов.
    
    Args:
        start: Начальная дата в формате ISO

        end: Конечная дата в формате ISO

        interval: Словарь с параметрами интервала
            Поддерживаются:
                - years,
                - months,
                - weeks,
                - days,
                - hours,
                - minutes,
                - seconds

        preserve_timezone: Сохранять ли исходный часовой пояс
    
    Returns:
        Список словарей с интервалами
    """
    output = list()
    with open('regions.log', 'r', encoding='utf-8') as file:
        lines = file.read().splitlines()
        regions = []
        for line in lines:
            elem = dict()
            _line = line.replace(';', '').replace('[', '').replace(']', '').split(' ')
            elem['region']      = _line[1]
            elem['date_from']   = _line[3]
            elem['date_to']     = _line[4]
            output.append(elem)
            regions.append(_line[1])
    return set(regions), output



# if __name__ == "__main__":
#     data = GetRegionsWithError()
#     for line in data:
#         # print(line)
#         test_line = GetDateTimeIntervales(line['date_from'], line['date_to'], {'hours': 6})
#         for interval in test_line:
#             print(interval)
#         break

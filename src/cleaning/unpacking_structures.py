from datetime import datetime
import re
import pandas as pd
import numpy as np
import json
# import os


def ProgressBar(value, min=0, max=100, SIZE=100):
    # SIZE = 100
    box = '▬'
    space = ' '
    boards = r'[]'
    _progress = value / (max - min)
    print(
        f"\r{boards[0]}{box * int(_progress * SIZE)}{space * (SIZE - int(_progress * SIZE))}{boards[1]} {round(_progress * 100, 2)} % ({value} of {max - min})\t", end='', flush=True
    )


def getList(column, by_key: str):
    if pd.isna(column):
        return None
    output = list()
    for elem in eval(column):
        # value = elem.replace("\'", '\"').replace('True', 'true')
        # value = value.replace("False", 'false').replace('None', 'null')
        # value = json.loads(elem)[by_key]
        value = elem.get(by_key, None).replace('\xa0', ' ')
        output.append(value)
            
    return output

def getValue(column, by_key: str, ignore_case=False):
    # print(type(column))
    if pd.isna(column):
        return np.nan

    try:
        if ignore_case:
            value = column.replace('True', 'true')
        else:
            value = column.replace("\'", '\"').replace('True', 'true')
        # value = value.replace('True', 'true')
        value = value.replace("False", 'false').replace('None', 'null')
        value = value.replace('\xa0', ' ')
        return json.loads(value).get(by_key, None)
    except Exception as e:
        print(f"Error: {e}")
        if ignore_case:
            value = column.replace('True', 'true')
        else:
            value = column.replace("\'", '\"').replace('True', 'true')
        value = value.replace('True', 'true')
        value = value.replace("False", 'false').replace('None', 'null')
        value = value.replace('\xa0', ' ')

        # print(column)
        print(value)
        exit()
        
        return None


def get_snippet(column, by_keys: list[str], get_key: str):
    keys_re = '|'.join(map(re.escape, by_keys))

    pattern = re.compile(
        rf"""
        ['"](?P<key>{keys_re})['"]
        \s*:\s*
        (?P<value>.*?)
        (?=
            \s*, \s*['"](?:{keys_re})['"]\s*:
            |
            \s*}}
        )
        """,
        re.VERBOSE | re.DOTALL
    )

    result = {}

    for mountain in pattern.finditer(column):
        value = mountain.group('value').split()

        if value and value[0] in "\"'" and value[-1] == value[0]:
            value = value[1:-1]

        result[mountain.group('key')] = value
    return result.get(get_key, None)


def get_employer(column, by_key: str):
    # data = column.split(', ')[1].replace('{', '').replace('}', '').replace('\'', '').split(': ')
    data = column.replace('{', '').replace('}', '').split('\', \'')
    try:
        data = {
            value.replace('\'', '').split(': ')[0]: value.replace('\'', '').split(': ')[1]
            for value in data
        }
        return data.get(by_key, None)
    except Exception as e:
        print(e)
        print(column)
        print([value.split(': ') for value in data])
        exit()
from .progress_bar import PrograsBar
from .sound import Sound

from datetime import datetime
import numpy as np
import pandas as pd
import json
import ast

SOUND: Sound = Sound()

def getList(column, by_key: str):
    output = list()
    if isinstance(column, list):
        
        for elem in column:
            value = elem.get(by_key, np.nan)
            output.append(value)
    else:
        try:
            # column = column.replace('\xa0', ' ')
            for elem in eval(column):
                value = elem.get(by_key, np.nan)
                output.append(value)
        except Exception as e:
            print(e)
            exit()
    return np.nan if (len(output) == 0) else output


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
        return json.loads(value).get(by_key, np.nan)
    except Exception as e:
        print(f"Error: {e}")
        
        
        return np.nan
    

def get_value(column, by_key):
    if pd.isna(column):
        return np.nan
    if isinstance(column, dict):
        return column.get(by_key, np.nan)
    else:
        try:
            data = ast.literal_eval(column)
            return data.get(by_key, np.nan)
        except Exception as e:
            print(f"Error: {e}")
            return np.nan

# def getJson(column, by_key):
#     if pd.isna(column):
#         return np.nan
    
#     try:
#         data = json.loads(column.replace('\'', '\"')).get(by_key, np.nan)
#         return data
#     except Exception as e:
#         print(e)
#         exit()

def getDateTime(column):
    try:
        dt_obj = datetime.fromisoformat(column)
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f") + dt_obj.strftime("%z")[:3]
    except Exception as e:
        print(e)
        return np.nan


def CleaningData(df: pd.DataFrame):

    # print(df.info())
    BAR: PrograsBar = PrograsBar(set_value=0, max=59, SIZE=50)
    
    BAR.show()
    # 0
    try:
        df['working_days_id'] = df['working_days'].apply(getList, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 1
    try:
        df['working_days_name'] = df['working_days'].apply(getList, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 2
    try:
        df['area_id'] = df['area'].apply(get_value, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 3
    try:
        df['area_name'] = df['area'].apply(get_value, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 4
    try:
        df['employment_id'] = df['employment'].apply(get_value, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 5
    try:
        df['employment_name'] = df['employment'].apply(get_value, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 6
    try:
        df['working_hours_id'] = df['working_hours'].apply(getList, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 7
    try:
        df['working_hours_name'] = df['working_hours'].apply(getList, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 8
    try:
        df['salary_from'] = df['salary'].apply(get_value, by_key='from')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 9
    try:
        df['salary_to'] = df['salary'].apply(get_value, by_key='to')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 10
    try:
        df['salary_currency'] = df['salary'].apply(get_value, by_key='currency')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 11
    try:
        df['gross'] = df['salary'].apply(get_value, by_key='gross')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 12
    try:
        df['snippet_requirement'] = df['snippet'].apply(get_value, by_key='requirement')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 13
    try:
        df['snippet_responsibility'] = df['snippet'].apply(get_value, by_key='responsibility')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 14
    try:
        df['employer_id'] = df['employer'].apply(get_value, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 15
    try:
        df['employer_name'] = df['employer'].apply(get_value, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 16
    try:
        df['employer_url'] = df['employer'].apply(get_value, by_key='url')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 17
    try:
        df['employer_alternate_url'] = df['employer'].apply(get_value, by_key='alternate_url')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 18
    try:
        df['employer_logo_urls_original'] = df['employer'].apply(get_value, by_key='logo_urls').apply(lambda x: x.get('original', np.nan) if isinstance(x, dict) else np.nan)
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 19
    try:
        df['employer_logo_urls_90'] = df['employer'].apply(get_value, by_key='logo_urls').apply(lambda x: x.get('90', np.nan) if isinstance(x, dict) else np.nan)
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 20
    try:
        df['employer_logo_urls_240'] = df['employer'].apply(get_value, by_key='logo_urls').apply(lambda x: x.get('240', np.nan) if isinstance(x, dict) else np.nan)
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 21
    try:
        df['employer_vacancies_url'] = df['employer'].apply(get_value, by_key='vacancies_url')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 22
    try:
        df['employer_accredited_it_employer'] = df['employer'].apply(get_value, by_key='accredited_it_employer')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 23
    try:
        df['employer_trusted'] = df['employer'].apply(get_value, by_key='trusted')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 24
    try:
        df['created_at_type'] = df['created_at'].apply(lambda x: datetime.fromisoformat(x.replace("+0300", "+03:00")))
        BAR.show(add=1)

        df['created_at_timestamptz'] = df['created_at'].apply(getDateTime)
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 25
    try:
        df['work_format_id'] = df['work_format'].apply(getList, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 26
    try:
        df['work_format_name'] = df['work_format'].apply(getList, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 27
    try:
        df['fly_in_fly_out_duration_id'] = df['fly_in_fly_out_duration'].apply(getList, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 28
    try:
        df['fly_in_fly_out_duration_name'] = df['fly_in_fly_out_duration'].apply(getList, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 29
    try:
        df['work_schedule_by_days_id'] = df['work_schedule_by_days'].apply(getList, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 30
    try:
        df['work_schedule_by_days_name'] = df['work_schedule_by_days'].apply(getList, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 31
    try:
        df['address_city'] = df['address'].apply(get_value, by_key='city')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 32
    try:
        df['address_street'] = df['address'].apply(get_value, by_key='street')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 33
    try:
        df['address_building'] = df['address'].apply(get_value, by_key='building')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 34
    try:
        df['address_lat'] = df['address'].apply(get_value, by_key='lat')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 35
    try:
        df['address_lon'] = df['address'].apply(get_value, by_key='lng')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 36
    try:
        df['address_description'] = df['address'].apply(get_value, by_key='description')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 37
    try:
        df['address_raw'] = df['address'].apply(get_value, by_key='raw')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 38
    try:
        df['address_metro'] = df['address'].apply(get_value, by_key='metro')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")
    
    # 39
    try:
        df['address_metro_stations'] = df['address'].apply(get_value, by_key='metro_stations')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")
    
    # 40
    try:
        df['address_id'] = df['address'].apply(get_value, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 41
    try:
        df['type_id'] = df['type'].apply(get_value, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 42
    try:
        df['type_name'] = df['type'].apply(get_value, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 43
    try:
        df['department_id'] = df['department'].apply(get_value, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 44
    try:
        df['department_name'] = df['department'].apply(get_value, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 45
    try:
        df['schedule_id'] = df['schedule'].apply(get_value, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 46
    try:
        df['schedule_name'] = df['schedule'].apply(get_value, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 47
    try:
        df['salary_range_from'] = df['salary_range'].apply(get_value, by_key='from')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 48
    try:
        df['salary_range_to'] = df['salary_range'].apply(get_value, by_key='to')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 49
    try:
        df['salary_range_currency'] = df['salary_range'].apply(get_value, by_key='currency')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 50
    try:
        df['salary_range_mode_id'] = df['salary_range'].apply(get_value, by_key='mode').apply(lambda x: x.get('id', np.nan) if isinstance(x, dict) else np.nan)
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 51
    try:
        df['salary_range_mode_name'] = df['salary_range'].apply(get_value, by_key='mode').apply(lambda x: x.get('name', np.nan) if isinstance(x, dict) else np.nan)
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 52
    try:
        df['salary_range_frequency_id'] = df['salary_range'].apply(get_value, by_key='frequency').apply(lambda x: x.get('id', np.nan) if isinstance(x, dict) else np.nan)
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 53
    try:
        df['salary_range_frequency_name'] = df['salary_range'].apply(get_value, by_key='frequency').apply(lambda x: x.get('name', np.nan) if isinstance(x, dict) else np.nan)
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 54
    try:
        df['working_time_intervals_id'] = df['working_time_intervals'].apply(getList, by_key='id')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # 55
    try:
        df['working_time_intervals_name'] = df['working_time_intervals'].apply(getList, by_key='name')
        BAR.show(add=1)
    except Exception as e:
        SOUND.signal()
        print(f"\n{e}\n")

    # TODO: replace published_at to DATATIME
    # print(df['published_at'].apply(getDateTime))
    BAR.show(add=1)

    
    check = df.shape
    to_drop = [
        'working_days', 'working_hours', 'area', 'employment', 'salary', 'snippet',
        'relations', 'employer', 'adv_response_url', 'created_at', 'work_format',
        'fly_in_fly_out_duration', 'work_schedule_by_days', 'insider_interview',
        'address', 'type', 'department', 'schedule', 'adv_context', 'sort_point_distance',
        'salary_range', 'working_time_intervals'
    ]
    df.drop(columns=to_drop, axis=1, inplace=True)
    BAR.show(add=1)
    del BAR
    print()

    return df
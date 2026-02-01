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
    BAR: PrograsBar = PrograsBar(set_value=0, max=69, SIZE=50)

    _key_get_list = [
        'working_days', 'working_hours', 'work_format', 'fly_in_fly_out_duration',
        'work_schedule_by_days', 'working_time_intervals', 'civil_law_contracts',
        'professional_roles'
    ]
    key_get_list = list(set(_key_get_list) & set(df.keys()))

    _key_get_value = [
        'area', 'employment', 'employer', 'type', 'department', 'schedule',
        'experience', 'employment_form'
    ]
    key_get_value = list(set(_key_get_value) & set(df.keys()))


    BAR.show()
    for _key in key_get_list:
        try:
            df[f'{_key}_id'] = df[_key].apply(getList, by_key='id')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df[f'{_key}_name'] = df[_key].apply(getList, by_key='name')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

    
    for _key in key_get_value:
        try:
            df[f'{_key}_id'] = df[_key].apply(get_value, by_key='id')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df[f'{_key}_name'] = df[_key].apply(get_value, by_key='name')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")


    if 'salary' in df.keys():
        try:
            df['salary_from'] = df['salary'].apply(get_value, by_key='from')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['salary_to'] = df['salary'].apply(get_value, by_key='to')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['salary_currency'] = df['salary'].apply(get_value, by_key='currency')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['gross'] = df['salary'].apply(get_value, by_key='gross')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

    if 'snippet' in df.keus():
        try:
            df['snippet_requirement'] = df['snippet'].apply(get_value, by_key='requirement')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['snippet_responsibility'] = df['snippet'].apply(get_value, by_key='responsibility')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")


    if 'employer' in df.keys():
        try:
            df['employer_url'] = df['employer'].apply(get_value, by_key='url')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['employer_alternate_url'] = df['employer'].apply(get_value, by_key='alternate_url')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['employer_logo_urls_original'] = df['employer'].apply(get_value, by_key='logo_urls').apply(lambda x: x.get('original', np.nan) if isinstance(x, dict) else np.nan)
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['employer_logo_urls_90'] = df['employer'].apply(get_value, by_key='logo_urls').apply(lambda x: x.get('90', np.nan) if isinstance(x, dict) else np.nan)
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['employer_logo_urls_240'] = df['employer'].apply(get_value, by_key='logo_urls').apply(lambda x: x.get('240', np.nan) if isinstance(x, dict) else np.nan)
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['employer_vacancies_url'] = df['employer'].apply(get_value, by_key='vacancies_url')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")


        try:
            df['employer_accredited_it_employer'] = df['employer'].apply(get_value, by_key='accredited_it_employer')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['employer_trusted'] = df['employer'].apply(get_value, by_key='trusted')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")


    if 'created_at' in df.keys():
        try:
            df['created_at'] = df['created_at'].apply(getDateTime)
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")


    if 'address' in df.keys():
        try:
            df['address_city'] = df['address'].apply(get_value, by_key='city')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['address_street'] = df['address'].apply(get_value, by_key='street')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['address_building'] = df['address'].apply(get_value, by_key='building')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['address_lat'] = df['address'].apply(get_value, by_key='lat')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['address_lon'] = df['address'].apply(get_value, by_key='lng')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['address_description'] = df['address'].apply(get_value, by_key='description')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['address_raw'] = df['address'].apply(get_value, by_key='raw')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['address_metro'] = df['address'].apply(get_value, by_key='metro')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['address_metro_stations'] = df['address'].apply(get_value, by_key='metro_stations')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['address_id'] = df['address'].apply(get_value, by_key='id')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

 

    if 'salary_range' in df.keys():
        try:
            df['salary_range_from'] = df['salary_range'].apply(get_value, by_key='from')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['salary_range_to'] = df['salary_range'].apply(get_value, by_key='to')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['salary_range_currency'] = df['salary_range'].apply(get_value, by_key='currency')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['salary_range_mode_id'] = df['salary_range'].apply(get_value, by_key='mode').apply(lambda x: x.get('id', np.nan) if isinstance(x, dict) else np.nan)
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['salary_range_mode_name'] = df['salary_range'].apply(get_value, by_key='mode').apply(lambda x: x.get('name', np.nan) if isinstance(x, dict) else np.nan)
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['salary_range_frequency_id'] = df['salary_range'].apply(get_value, by_key='frequency').apply(lambda x: x.get('id', np.nan) if isinstance(x, dict) else np.nan)
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        try:
            df['salary_range_frequency_name'] = df['salary_range'].apply(get_value, by_key='frequency').apply(lambda x: x.get('name', np.nan) if isinstance(x, dict) else np.nan)
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")


    BAR.show(add=1)

    if 'published_at' in df.keys():
        try:
            df['published_at'] = df['published_at'].apply(getDateTime)
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")
    
    
    if 'branding' in df.keys():
        try:
            df['branding_type'] = df['branding'].apply(get_value, by_key='type')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")

        # 58
        try:
            df['branding_tariff'] = df['branding'].apply(get_value, by_key='tariff')
            BAR.show(add=1)
        except Exception as e:
            SOUND.signal()
            print(f"\n{e}\n")
    
    
    
    check = df.shape
    _to_drop = [
        'working_days', 'working_hours', 'area', 'employment', 'salary', 'snippet',
        'relations', 'employer', 'adv_response_url', 'branding', 'work_format',
        'fly_in_fly_out_duration', 'work_schedule_by_days', 'insider_interview',
        'address', 'type', 'department', 'schedule', 'adv_context', 'sort_point_distance',
        'salary_range', 'working_time_intervals'
    ]

    to_drop = list(set(_to_drop) & set(df.keys()))

    df.drop(columns=to_drop, axis=1, inplace=True)
    BAR.show(add=1)
    del BAR
    print()

    return df
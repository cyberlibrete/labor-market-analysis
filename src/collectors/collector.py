from collectors.hh_api import HHClient
from collectors.superjob_api import SuperJobAPI

import pandas as pd
import os

from datelines import *


def get_hh_areas_data():
    areas = HHClient.get_area()

    data_of_areas = {
        "country_id": [],
        "country_name": [],
        "region_id": [],
        "region_name": [],
        "city_id": [],
        "city_name": []
    }

    csv_countries = {
        "id": [],
        "name": [],
    }
    csv_regions = {
        "country_id": [],
        "id": [],
        "name": [],
    }
    csv_city = {
        "country_id": [],
        "region_id": [],
        "id": [],
        "name": [],
    }
    
    for item in areas:
        csv_countries["id"].append(item["id"])
        csv_countries["name"].append(item["name"])

        for area in item["areas"]:
            csv_regions["country_id"].append(item["id"])
            csv_regions["id"].append(area["id"])
            csv_regions["name"].append(area["name"])

            for region in area['areas']:
                csv_city["country_id"].append(item["id"])
                csv_city["region_id"].append(area["id"])
                csv_city["id"].append(region["id"])
                csv_city["name"].append(region["name"])

                data_of_areas["country_id"].append(item["id"])
                data_of_areas["country_name"].append(item["name"])
                data_of_areas["region_id"].append(area["id"])
                data_of_areas["region_name"].append(area["name"])
                data_of_areas["city_id"].append(region["id"])
                data_of_areas["city_name"].append(region["name"])
                


    df = pd.DataFrame(data=data_of_areas)
    df_countries = pd.DataFrame(data=csv_countries)
    df_regions = pd.DataFrame(data=csv_regions)
    df_cities = pd.DataFrame(data=csv_city)
    print(df.info())

    df.to_csv("./data/HH/hh-full-list.csv", index=False)
    df_countries.to_csv("./data/HH/hh-countries.csv", index=False)
    df_regions.to_csv("./data/HH/hh-regions.csv", index=False)
    df_cities.to_csv("./data/HH/hh-city.csv", index=False)
    
    del df
    del df_countries
    del df_regions
    del df_cities



def saving_data_to_file(_country: dict, _district: dict, _timePeriod: dict, data: list, path: str):
    if len(data) > 0:
        print("[v] SAVING {} elements of district {} ({}) by priod [{} {}]".format(
            len(data),
            _district['id'],
            _district['name'],
            _timePeriod['date_from'],
            _timePeriod['date_to']
        ))
        
        # Определяем переменную для списка примененных признаков
        fieldnames = list()

        for it in data:
            fieldnames += it.keys()
        fieldnames = list(set(fieldnames))
        output_data = {f"{key}": [] for key in fieldnames}
        
        for line in data:
            for _key in fieldnames:
                output_data[f"{_key}"].append(line.get(f"{_key}", ""))
        
        # Создаем датафрейм (таблицу вакансий в регионе)
        df = pd.DataFrame(data=output_data)

        __timePeriod = f"{_timePeriod['date_from']}_{_timePeriod['date_to']}".replace('+03:00', '').replace(':', '-')

        df.to_csv(f"{path}{_country['id']}_{_district['id']}_{__timePeriod}.csv", index=False)
        del df

def recurs_request(_mod: HHClient, country, district, date_from, date_to, period_lvl: int = 1, inChunks = True):
    output = []

    if not setLevelDate(period_lvl): return []
    
    for timePeriod in GetDateTimeIntervales(date_from, date_to, setLevelDate(period_lvl)):
        _output = []
        __timeperiod = f"{timePeriod['date_from']}_{timePeriod['date_to']}".replace('+03:00', '').replace(':', '-')
        check_path = f"./data/raw/HH/vacancies/{country['id']}_{district['id']}_{__timeperiod}.csv"
        # print(check_path)

        if os.path.exists(check_path):
            continue
        try:
            _temp_output    = []
            page_id         = 0
            # periodIsSuccess = False

            while True:
        
                item = _mod.serach(
                    area=district['id'],
                    page=page_id,
                    per_page=100,
                    date_from=timePeriod['date_from'],
                    date_to=timePeriod['date_to']
                ).get("items", [])

                print(" {} check: {}({}) period: [{} {}] elements: {} +{}".format(
                    f"{'-'*(period_lvl - 1)}{period_lvl}{'-'*(8 - period_lvl)}",
                    district['id'],
                    page_id,
                    timePeriod['date_from'],
                    timePeriod['date_to'],
                    len(_temp_output),
                    len(item)
                ))

                # periodIsSuccess = True
                if len(item) == 0:
                    break

                _temp_output.extend(item)
                page_id += 1

            if len(_temp_output) > 0:
                print("LOADED: {} +{}".format(
                    len(_output),
                    len(_temp_output)
                ))
                _output.extend(_temp_output)
            if inChunks:
                saving_data_to_file(country, district, timePeriod, _output, './data/raw/HH/vacancies/')
        except Exception as e:
            print(f"ERROR: {e}")
            print('[with page: {}]'.format(page_id))
            # periodIsSuccess = False
            print("SPLIT: {} [{} {}] {}".format(
                district['id'],
                timePeriod.get('date_from'),
                timePeriod.get('date_to'),
                (period_lvl+1)
            ))
            _output = recurs_request(
                _mod,
                country,
                district,
                date_from=timePeriod.get('date_from'),
                date_to=timePeriod.get('date_to'),
                period_lvl=(period_lvl+1)
            )
            # if len(_temp_output) > 0:
            #     _output.extend(_temp_output)
        if not inChunks:
            output.extend(_output)

    if not inChunks:
        return output
    else:
        return True



def get_vacancies_by_area(_mod: HHClient):
    DateTimeNow = dt.datetime.now().strftime(r"%Y-%m-%dT%H:%M:%S.00+03:00")
    DateTimeLines = GetDateTimeIntervales(
        '2025-01-01T00:00:00.00+03:00',
        '2025-12-31T23:59:59.00+03:00',
        {'days': 3}
    ) + GetDateTimeIntervales(
        '2026-01-01T00:00:00.00+03:00',
        DateTimeNow,
        {'minutes': 10})
    
    PATH = './data/raw/HH/vacancies/'
    

    # Получаем дерево регионов
    regions = _mod.get_area()

    # Обрабатываем каждый ID страны
    for country in regions:
        # if (country['id'] not in ['113', '16']):
        #     continue

        # check_reg, _ = GetRegionsWithError()
        # Обрабатываем каждый ID региона
        for district in country["areas"]:

            # Список данных по вакансиям
            vacancies_data = []
            print(f"{district['id']}: {district['name']}")
            vacancies_data = recurs_request(
                _mod,
                country,
                district,
                '2025-01-01T00:00:00.00+03:00',
                DateTimeNow,
                1
            )
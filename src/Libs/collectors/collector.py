from .hh_api import HHClient
from ..algorithms.progress_bar import PrograsBar
# from collectors.superjob_api import SuperJobAPI

import requests
import threading
import pandas as pd
import os

from ..algorithms import GetDateTimeIntervales, setLevelDate, CleaningData
# from ..algorithms.timeline import *

class ParserHeadHunter:
    def __init__(
            self,
            # active_object: HHClient,
            date_interval_from: str,
            date_interval_to: str,
            saving_postgresql = None,
            saving_file = None
        ):

        self.active_object          = HHClient()
        self.date_interval_from     = date_interval_from
        self.date_interval_to       = date_interval_to
        self.saving_postgresql      = saving_postgresql
        self.saving_file            = saving_file
        self.__data_path            = './data/HH/vacancies/'
        self.__saving_by_chunks     = True
        self.__verb                 = False
        self.__dir_exists           = self.__creat_dir()
        self.__cleaning_data        = False
        self.__CHUNK_SIZE           = 10

    def __check_dir(self):
        return os.path.exists(self.__data_path)
    
    
    def __creat_dir(self):
        if not self.__check_dir():
            os.makedirs(self.__data_path)
        return self.__check_dir()
    

    def switchSavingByChunks(self, _type: bool) -> bool:
        self.__saving_by_chunks = _type
        return True
    
    
    def switchVerbouse(self, _type: bool):
        self.__verb = _type
        return True
    
    def switchCleaningData(self, _type: bool):
        self.__cleaning_data = _type
        return True
    
    
    def _SavingDataToCsv(self, _country: dict, _district: dict, _timePeriod: dict, data: list):
        if not self.__check_dir():
            self.__creat_dir()
        if len(data) > 0:
            if self.__verb:
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

            df.to_csv(f"{self.__data_path}{_country['id']}_{_district['id']}_{__timePeriod}.csv", index=False)
            del df
        
    def SavingDataToCsv(self, _country: dict, _district: dict, _timePeriod: dict, data: list):
        if len(data) > 0:
            # Создаем датафрейм (таблицу вакансий в регионе)
            df = pd.DataFrame(data)
            if self.__cleaning_data:
                df = CleaningData(df)

            __timePeriod = f"{_timePeriod['date_from']}_{_timePeriod['date_to']}".replace('+03:00', '').replace(':', '-')
            filename = "{}{}{}_{}_{}.csv".format(
                self.__data_path,
                'cl_' if self.__cleaning_data else '',
                _country['id'],
                _district['id'],
                __timePeriod
            )

            df.to_csv(filename, index=False)
            del df

        
    def __recursRequest(self, country, district, date_from, date_to, period_lvl: int = 1):
        output = []

        if not setLevelDate(period_lvl): return []

        DATELINE_LIST = GetDateTimeIntervales(date_from, date_to, period_lvl)

        for timePeriod in DATELINE_LIST:
            _output = []

            try:
                _temp_output    = []
                page_id         = 0
                # periodIsSuccess = False

                while True:
            
                    item = self.active_object.serach(
                        area=district['id'],
                        page=page_id,
                        per_page=100,
                        date_from=timePeriod['date_from'],
                        date_to=timePeriod['date_to']
                    ).get("items", [])
                    
                    if self.__verb:
                        print("LEVEL [{}] DISTCRICT: {}.{} OF TIMELINE: [{} {}] FOUND: {} NEW ELEMENTS: {}".format(
                            f"{'-'*(period_lvl - 1)}{period_lvl}{'-'*(8 - period_lvl)}",
                            f"{district['id']: >5}",
                            page_id,
                            timePeriod['date_from'].replace('T', ' '),
                            timePeriod['date_to'].replace('T', ' '),
                            len(_temp_output),
                            len(item)
                        ))

                    if len(item) == 0:
                        break

                    _temp_output.extend(item)
                    page_id += 1

                if len(_temp_output) > 0:
                    if self.__verb:
                        print("LOADED: {} +{}".format(
                            len(_output),
                            len(_temp_output)
                        ))
                    _output.extend(_temp_output)
                if self.__saving_by_chunks:
                    _BAR: PrograsBar = PrograsBar(set_value=0, box='•', max=len(_output), SIZE=50)

                    def __deep_loading(raw):
                        try:
                            _temp_url = raw.get('url')
                            _temp_data = requests.request(method='GET', url=_temp_url).json()
                            all_keys = list(set(list(raw.keys()) + list(_temp_data.keys())))
                            

                            for _key in all_keys:
                                key_a = _key in raw.keys()
                                key_b = _key in _temp_data.keys()
                                equal_data = raw.get(_key, None) == _temp_data.get(_key, None)
                                
                                if (not key_a) and (key_b):
                                    raw[_key] = _temp_data.get(_key)

                                elif (key_a) and (key_b) and (equal_data):
                                    raw[f"adv__{_key}"] = _temp_data.get(_key)

                                else:
                                    continue
                            _BAR.show(add=1)
                        except Exception as e:
                            print(f'\n--> ERROR {e}')

                    # self.__CHUNK_SIZE = 5
                    check_line = [x for x in range(0, len(_output), self.__CHUNK_SIZE)]

                    for chunk in check_line:
                        a = chunk
                        b = (chunk + self.__CHUNK_SIZE) if (chunk + self.__CHUNK_SIZE) <= len(_output) else len(_output)
                        # current_loading = _output[a:b]
                        process_list = [threading.Thread(target=__deep_loading, args=(row,)) for row in _output[a:b]]

                        for process in process_list:
                            process.start()
                        for process in process_list:
                            process.join()
                    
                    self.SavingDataToCsv(country, district, timePeriod, _output)
            except Exception as e:
                if self.__verb:
                    print(f"ERROR: {e}")
                    print('[with page: {}]'.format(page_id))
                    print("SPLIT: {} [{} {}] {}".format(
                        district['id'],
                        timePeriod.get('date_from'),
                        timePeriod.get('date_to'),
                        (period_lvl+1)
                    ))

                _output = self.__recursRequest(
                    country,
                    district,
                    date_from=timePeriod.get('date_from'),
                    date_to=timePeriod.get('date_to'),
                    period_lvl=(period_lvl+1)
                )

            if not self.__saving_by_chunks:
                output.extend(_output)

        if not self.__saving_by_chunks:
            self.SavingDataToCsv(country, district, timePeriod, _output)
        else:
            return True


    def getVacancies(
            self,
            countries: list = None,
            districts: list = None
        ):
        regions = self.active_object.get_area()

        if countries:
            regions = [
                country
                for country in regions
                if country.get('id') in countries
            ]

        for country in regions:
            
            district_list = country["areas"]

            if districts:
                district_list = [
                    distr
                    for distr in country["areas"]
                    if country["areas"].get('id') in districts
                ]

            for district in district_list:
                # Список данных по вакансиям
                if self.__verb:
                    print(f"{district['id']}: {district['name']}")
                self.__recursRequest(
                    country,
                    district,
                    self.date_interval_from,
                    self.date_interval_to,
                    1
                )




# def SavingDataToCsv(_country: dict, _district: dict, _timePeriod: dict, data: list, path: str):
#     if len(data) > 0:
#         print("[v] SAVING {} elements of district {} ({}) by priod [{} {}]".format(
#             len(data),
#             _district['id'],
#             _district['name'],
#             _timePeriod['date_from'],
#             _timePeriod['date_to']
#         ))
        
#         # Определяем переменную для списка примененных признаков
#         fieldnames = list()

#         for it in data:
#             fieldnames += it.keys()
#         fieldnames = list(set(fieldnames))
#         output_data = {f"{key}": [] for key in fieldnames}
        
#         for line in data:
#             for _key in fieldnames:
#                 output_data[f"{_key}"].append(line.get(f"{_key}", ""))
        
#         # Создаем датафрейм (таблицу вакансий в регионе)
#         df = pd.DataFrame(data=output_data)

#         __timePeriod = f"{_timePeriod['date_from']}_{_timePeriod['date_to']}".replace('+03:00', '').replace(':', '-')

#         df.to_csv(f"{path}{_country['id']}_{_district['id']}_{__timePeriod}.csv", index=False)
#         del df

# def recurs_request(_mod: HHClient, country, district, date_from, date_to, period_lvl: int = 1, inChunks = True):
#     output = []

#     if not setLevelDate(period_lvl): return []
#     DATELINE_LIST = GetDateTimeIntervales(date_from, date_to)
#     for timePeriod in GetDateTimeIntervales(date_from, date_to, setLevelDate(period_lvl)):
#         _output = []
#         __timeperiod = f"{timePeriod['date_from']}_{timePeriod['date_to']}".replace('+03:00', '').replace(':', '-')
#         check_path = f"./data/raw/HH/vacancies/{country['id']}_{district['id']}_{__timeperiod}.csv"
#         # print(check_path)

#         if os.path.exists(check_path):
#             continue
#         try:
#             _temp_output    = []
#             page_id         = 0
#             # periodIsSuccess = False

#             while True:
        
#                 item = _mod.serach(
#                     area=district['id'],
#                     page=page_id,
#                     per_page=100,
#                     date_from=timePeriod['date_from'],
#                     date_to=timePeriod['date_to']
#                 ).get("items", [])

#                 print(" {} check: {}({}) period: [{} {}] elements: {} +{}".format(
#                     f"{'-'*(period_lvl - 1)}{period_lvl}{'-'*(8 - period_lvl)}",
#                     district['id'],
#                     page_id,
#                     timePeriod['date_from'],
#                     timePeriod['date_to'],
#                     len(_temp_output),
#                     len(item)
#                 ))

#                 # periodIsSuccess = True
#                 if len(item) == 0:
#                     break

#                 _temp_output.extend(item)
#                 page_id += 1

#             if len(_temp_output) > 0:
#                 print("LOADED: {} +{}".format(
#                     len(_output),
#                     len(_temp_output)
#                 ))
#                 _output.extend(_temp_output)
#             if inChunks:
#                 SavingDataToCsv(country, district, timePeriod, _output, './data/raw/HH/vacancies/')
#         except Exception as e:
#             print(f"ERROR: {e}")
#             print('[with page: {}]'.format(page_id))
#             # periodIsSuccess = False
#             print("SPLIT: {} [{} {}] {}".format(
#                 district['id'],
#                 timePeriod.get('date_from'),
#                 timePeriod.get('date_to'),
#                 (period_lvl+1)
#             ))
#             _output = recurs_request(
#                 _mod,
#                 country,
#                 district,
#                 date_from=timePeriod.get('date_from'),
#                 date_to=timePeriod.get('date_to'),
#                 period_lvl=(period_lvl+1)
#             )
#             # if len(_temp_output) > 0:
#             #     _output.extend(_temp_output)
#         if not inChunks:
#             output.extend(_output)

#     if not inChunks:
#         return output
#     else:
#         return True



# def get_vacancies_by_area(_mod: HHClient):
#     DateTimeNow = dt.datetime.now().strftime(r"%Y-%m-%dT%H:%M:%S.00+03:00")
#     DateTimeLines = GetDateTimeIntervales(
#         '2025-01-01T00:00:00.00+03:00',
#         '2025-12-31T23:59:59.00+03:00',
#         {'days': 3}
#     ) + GetDateTimeIntervales(
#         '2026-01-01T00:00:00.00+03:00',
#         DateTimeNow,
#         {'minutes': 10})
    
#     PATH = './data/raw/HH/vacancies/'
    

#     # Получаем дерево регионов
#     regions = _mod.get_area()

#     # Обрабатываем каждый ID страны
#     for country in regions:
#         # if (country['id'] not in ['113', '16']):
#         #     continue

#         # check_reg, _ = GetRegionsWithError()
#         # Обрабатываем каждый ID региона
#         for district in country["areas"]:

#             # Список данных по вакансиям
#             vacancies_data = []
#             print(f"{district['id']}: {district['name']}")
#             vacancies_data = recurs_request(
#                 _mod,
#                 country,
#                 district,
#                 '2025-01-01T00:00:00.00+03:00',
#                 DateTimeNow,
#                 1
#             )
from collectors.hh_api import HHClient
from collectors.superjob_api import SuperJobAPI
import pandas as pd

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

def get_vacancies_by_area(_mod: HHClient):
    _data = _mod.get_area()
    for country in _data:
        print(country["id"], country["name"])
        for district in country["areas"]:
            # print(district["id"], district["name"])

            vacancies_data = []
            _page_idx = 0
            while True:
                print(f"district_id: {district["id"]}\t\tpage: {_page_idx}")
                try:
                    items = _mod.serach(
                        area=district["id"],
                        page=_page_idx
                    ).get("items", [])

                    if len(items) == 0:
                        break
                    
                    vacancies_data.extend(items)

                except Exception as e:
                    print("ERROR: {e}")
                    break

                _page_idx += 1
                break
            
            if len(vacancies_data) > 0:
                print(
                    f"saving data with vacancies of district {district["id"]} ({district["name"]})"
                )
                
                fieldnames = list()
                for it in vacancies_data:
                    fieldnames += it.keys()
                fieldnames = list(set(fieldnames))
                output_data = {f"{key}": [] for key in fieldnames}
                
                for line in vacancies_data:
                    for _key in fieldnames:
                        output_data[f"{_key}"].append(line.get(f"{_key}", ""))
                
                df = pd.DataFrame(data=output_data)
                
                # TODO: описать очитску данных

                df.to_csv(f"./data/HH/vacancies/{district["id"]}.csv", index=True)
                print(df)

                
                del df

            break
        break

def main():
    hh = HHClient()

    get_vacancies_by_area(hh)

    

    

if __name__ == "__main__":
    main()
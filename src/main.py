from Libs.collectors.collector import ParserHeadHunter
import datetime as dt



def main():
    DateTimeNow = dt.datetime.now().strftime(r"%Y-%m-%dT%H:%M:%S.00+03:00")
    OBJECT: ParserHeadHunter = ParserHeadHunter(
        date_interval_from='2025-01-01T00:00:00.00+03:00',
        date_interval_to=DateTimeNow,
        saving_file=True
    )

    OBJECT.switchVerbouse(True)
    OBJECT.switchCleaningData(True)
    OBJECT.getVacancies()

    

    

if __name__ == "__main__":
    main()


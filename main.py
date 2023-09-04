# создание папок Downloads и Archives на уровень yaml

# 1 обработка URL
# 2 получение архива
# 3 обработка архива

import os
import logging
import datetime
from config import Config
from modules.stg_loader import StgControler
from modules.instrumentals import years_extractor, get_stations, get_unfield_stations
from modules.dds_loader import DdsControler

config = Config()
log = logging.getLogger(__name__)

[os.remove(file) for file in os.listdir(os.getcwd()) if file.endswith('.csv')]


def weather_date(controler: StgControler,
                 start_date: str,
                 end_date: str) -> None:
    years = years_extractor(start_date=start_date, end_date=end_date)

    all_stations_id = get_stations(db_connection=config.pg_warehouse_db())
    print(len(all_stations_id))
    log.info(f"The calculation will be performed for {' ,'.join([str(x) for x in years])} year(years)")
    print(f"The calculation will be performed for {' ,'.join([str(x) for x in years])} year(years)")

    for year in years:
        unfield_stations = get_unfield_stations(db_connection=config.pg_warehouse_db(), year=year)
        print(len(unfield_stations))
        log.info(f"Selection for {year} year is running")
        print(f"Selection for {year} year is running")
        choosen_station = list(set(all_stations_id) - set(unfield_stations))
        for index in range(len(choosen_station)):
            log.info(f" {len(choosen_station) - index} stations left to handle for {year} year")
            print(f" {len(choosen_station) - index} stations left to handle for {year} year")
            controler.receive_weatherstation_data(year=year, station_id=choosen_station[index])
            controler.load_weatherstation_data(table_name="weather_observation", station_id=choosen_station[index])


def animal_incidents_data(controler: StgControler,
                          start_date: str,
                          end_date: str):
    controler.receive_animal_incidents_data(start_date=start_date, end_date=end_date)
    controler.unzip_data()
    controler.download_incidents(table_name='aircraft_incidents')


if __name__ == '__main__':
    stg_loadings = StgControler(date=datetime.datetime.now().date(),
                                pg_connect=config.pg_warehouse_db(),
                                schema='Stage',
                                logger=log)
    weather_date(controler=stg_loadings, start_date='2019-01-01', end_date='2019-12-31')
    # animal_incidents_data(controler=stg_loadings, start_date='2022-01-01', end_date='2022-12-31')
    dds_uploads = DdsControler(date=datetime.datetime.now().date(),
                               pg_connect=config.pg_warehouse_db(),
                               schema='DDS',
                               logger=log)
    dds_uploads.upload_aircraft_incidents(table_name='aircraft_incidents')
    dds_uploads.upload_weather_observation(table_name='weather_observation')

    #cdm
    """
    SELECT airport_id, airport, count(*), now()::date as processed_dt FROM DDS.aircraft_incidents GROUP BY airport_id, airport ORDER BY 3 DESC LIMIT 10;

    """

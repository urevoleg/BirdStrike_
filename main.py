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
from modules.cdm_loader import CdmControler

config = Config()
log = logging.getLogger(__name__)

[os.remove(file) for file in os.listdir(os.getcwd()) if file.endswith('.csv')]


def weather_date(controller: StgControler,
                 start_date: str,
                 end_date: str) -> None:
    years = years_extractor(start_date=start_date, end_date=end_date)

    all_stations_id = get_stations(db_connection=config.pg_warehouse_db())
    print(len(all_stations_id))
    log.info(f"The calculation will be performed for {' ,'.join([str(x) for x in years])} year(years)")
    print(f"The calculation will be performed for {' ,'.join([str(x) for x in years])} year(years)")

    for year in years:
        unfield_stations = get_unfield_stations(db_connection=config.pg_warehouse_db(), year=year)
        log.info(f"Selection for {year} year is running")
        print(f"Selection for {year} year is running")
        choosen_station = list(set(all_stations_id) - set(unfield_stations))
        for index in range(len(choosen_station)):
            log.info(f" {len(choosen_station) - index} stations left to handle for {year} year")
            print(f" {len(choosen_station) - index} stations left to handle for {year} year")
            controller.receive_weatherstation_data(year=year, station_id=choosen_station[index])
            controller.load_weatherstation_data(table_name="weather_observation", station_id=choosen_station[index])


def animal_incidents_data(controller: StgControler,
                          start_date: str,
                          end_date: str):
    controller.receive_animal_incidents_data(start_date=start_date, end_date=end_date)
    controller.unzip_data()
    controller.download_incidents(table_name='aircraft_incidents')


def top_airports(controller: StgControler, process_date):
    with config.pg_warehouse_db().connection() as connect:
        cursor = connect.cursor()
        query = """
        with cte as(
        SELECT airport_id, airport, count(*), now()::date as processed_dt FROM DDS.aircraft_incidents
        GROUP BY airport_id, airport ORDER BY 3 DESC LIMIT 11) -- 11 из-за UNKNOWN но стоит обсудить с аналитиками нужен ли им этот "аэропорт"
        SELECT cte.airport_id, airport, bts_name FROM cte
        INNER JOIN DDS.airport_bts_name --UNKNOWN на INNER JOIN отфильтруется
            ON cte.airport_id = airport_bts_name.id;
        """
        cursor.execute(query)
        tuples_airports = [(x[0], x[1], x[2])for x in cursor.fetchall()]
        controller.top_airports_traffic(table_name='top_ten_airports', airports_data=tuples_airports,
                                        process_date=process_date)


if __name__ == '__main__':
    stg_loadings = StgControler(date=datetime.datetime.now().date(),
                                pg_connect=config.pg_warehouse_db(),
                                schema='Stage',
                                logger=log)
    dds_uploads = DdsControler(date=datetime.datetime.now().date(),
                               pg_connect=config.pg_warehouse_db(),
                               schema='DDS',
                               logger=log)
    cdm_uploads = CdmControler(date=datetime.datetime.now().date(),
                               pg_connect=config.pg_warehouse_db(),
                               schema='CDM',
                               logger=log)

    weather_date(controller=stg_loadings, start_date='2021-01-01', end_date='2021-12-31')
    #animal_incidents_data(controller=stg_loadings, start_date='2022-01-01', end_date='2022-12-31')

    #dds_uploads.upload_aircraft_incidents(table_name='aircraft_incidents')
    #dds_uploads.upload_weather_observation(table_name='weather_observation', date='2020-12-31')

    #top_airports(controller=stg_loadings, process_date=datetime.datetime.now().date())



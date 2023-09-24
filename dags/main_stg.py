import os
import logging
import datetime
from modules.config import Config
from modules.stg_loader import StgControler
from airflow import DAG
from airflow.operators.python import PythonOperator

config = Config()
log = logging.getLogger(__name__)

[os.remove(file) for file in os.listdir(os.getcwd()) if file.endswith('.csv')]
[os.mkdir(name) for name in ["Archives", "Downloads", "Unresolved"] if name not in os.listdir()]


def weather_data(controller: StgControler,
                 start_date: str,
                 end_date: str) -> None:
    with controller.pg_connect.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"""TRUNCATE TABLE STAGE.weather_observation""")
        log.info(f"Total start = {start_date}")
        log.info(f"Total end = {end_date}")
        query = f"""
            SELECT DISTINCT indx_nr, incident_date, time, weather_station
            FROM DDS.aircraft_incidents
            INNER JOIN DDS.incident_station_link link ON aircraft_incidents.indx_nr=link.index_incedent
            WHERE incident_date between '{start_date}' and '{end_date}'
            AND indx_nr not in (SELECT distinct cast(incident as int)
                                FROM DDS.weather_observation)
            ORDER BY incident_date ASC"""
        cursor.execute(query)
        records = cursor.fetchall()
        log.info(f'Выборка записей {len(set(records))}')

    # Обрабатываем небольшими партиями, API не принимает более 50 stations
    for i in range(len(records[:50])):
        min_date = min([x[1] for x in records[:50]])
        log.info(f'Минимальная дата в партиции: {min_date}')
        max_date = max([x[1] for x in records[:50]])
        log.info(f'Максимальная дата в партиции: {max_date}')
        stations = [x[3] for x in records[:50]]

        controller.receive_weatherstation_data(station_id=','.join(list(set(stations[:50]))),
                                               start_datetime=min_date - datetime.timedelta(hours=1),
                                               end_datetime=max_date + datetime.timedelta(hours=1))
        controller.load_weatherstation_data(table_name="weather_observation")
        records = records[50:]


def animal_incidents_data(controller: StgControler,
                          start_date: str = None,
                          end_date: str = None):
    controller.receive_animal_incidents_data(start_date=start_date, end_date=end_date)
    controller.unzip_data()
    controller.download_incidents(table_name='aircraft_incidents')


stg_loadings = StgControler(date=datetime.datetime.now().date(),
                            pg_connect=config.pg_warehouse_db(),
                            schema='Stage',
                            logger=log)

with DAG(
        dag_id="Stage_upload",
        tags=['Bird_strike', 'Stage'],
        start_date=datetime.datetime(2018, 1, 1),
        max_active_runs=1,
        schedule="5 4 * * 3",
        catchup=False,
        default_args={
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=3)}
) as dag:
    task_animal_incidents = PythonOperator(
        task_id='download_animal_incidents',
        python_callable=animal_incidents_data,
        op_kwargs={'controller': stg_loadings,
                   'start_date': '2022-01-01',
                   'end_date': '2022-12-31'})
    task_weather_data = PythonOperator(
        task_id='download_weather_data',
        python_callable=weather_data,
        op_kwargs={'controller': stg_loadings,
                   'start_date': '2022-01-01',
                   'end_date': '2022-12-31'})

[task_animal_incidents, task_weather_data]

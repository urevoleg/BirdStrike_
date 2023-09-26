import os
import logging
import datetime
from config import Config
from modules.stg_loader import StgControler
from modules.dds_loader import DdsControler
from modules.cdm_loader import CdmControler
from airflow import DAG
from airflow.operators.python import PythonOperator

config = Config()
log = logging.getLogger(__name__)

[os.remove(file) for file in os.listdir(os.getcwd()) if file.endswith('.csv')]
[os.mkdir(name) for name in ["Archives", "Downloads", "Unresolved"] if name not in os.listdir()]


def weather_data(controller: StgControler, start_date, end_date) -> None:
    with controller.pg_connect.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"""TRUNCATE TABLE STAGE.weather_observation""")  # Clean up stage
        log.info(f"Total start_date = {start_date}")
        log.info(f"Total end_date = {end_date}")
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
        log.info(f'Number of incidents, whose dont have weather data: {len(set(records))}')
    # API can receive only 50 stations at once, so dag take batch with for 50 incidents at once
    for i in range(len(records[:50])):
        min_date = min([x[1] for x in records[:50]])
        log.info(f'Minimal date in batch: {min_date}')
        max_date = max([x[1] for x in records[:50]])
        log.info(f'Maximum date in batch: {max_date}')
        stations = [x[3] for x in records[:50]]

        controller.receive_weatherstation_data(stations_id=','.join(list(set(stations[:50]))),
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
dds_uploads = DdsControler(date=datetime.datetime.now().date(),
                           pg_connect=config.pg_warehouse_db(),
                           schema='DDS',
                           logger=log)
cdm_loader = CdmControler(date=datetime.datetime.now().date(),
                          pg_connect=config.pg_warehouse_db(),
                          schema='CDM',
                          logger=log)

with DAG(
        dag_id="FULL_DAG",
        tags=['BirdStrike'],
        start_date=datetime.datetime(2018, 1, 1),
        max_active_runs=1,
        schedule="5 4 * * 3",
        catchup=False,
        default_args={
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=3)}
) as dag:
    task_observation_reference = PythonOperator(
        task_id='download_observation_reference',
        python_callable=stg_loadings.isd_history,
        op_kwargs={'table_name': 'observation_reference'
                   })

    task_animal_incidents = PythonOperator(
        task_id='download_animal_incidents',
        python_callable=animal_incidents_data,
        op_kwargs={'controller': stg_loadings,
                   'start_date': '2018-01-01'
                   })
    task_weather_data = PythonOperator(
        task_id='download_weather_data',
        python_callable=weather_data,
        op_kwargs={'controller': stg_loadings,
                   'start_date': datetime.datetime(year=2019, month=1, day=1),
                   'end_date': datetime.datetime(year=2019, month=12, day=31)})
    upload_animal_incidents = PythonOperator(
        task_id='upload_animal_incidents',
        python_callable=dds_uploads.upload_aircraft_incidents,
        op_kwargs={'table_name': 'aircraft_incidents'})
    upload_weather_data = PythonOperator(
        task_id='upload_weather_data',
        python_callable=dds_uploads.upload_weather_observation,
        op_kwargs={'table_name': 'weather_observation'})
    upload_weather_reference = PythonOperator(
        task_id='upload_weather_reference',
        python_callable=dds_uploads.upload_weather_reference,
        op_kwargs={'table_name': 'observation_reference'})
    upload_incident_station_link = PythonOperator(
        task_id='upload_incident_station_link',
        python_callable=dds_uploads.update_incident_station_link,
        op_kwargs={'table_name': 'incident_station_link'})

task_observation_reference >> upload_weather_reference >> upload_incident_station_link
task_animal_incidents >> upload_animal_incidents >> upload_incident_station_link
upload_incident_station_link >> task_weather_data >> upload_weather_data

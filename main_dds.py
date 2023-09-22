import os
import logging
import datetime
from config import Config
from modules.dds_loader import DdsControler
from modules.cdm_loader import CdmControler
from airflow import DAG
from airflow.operators.python import PythonOperator

config = Config()
log = logging.getLogger(__name__)

dds_uploads = DdsControler(date=datetime.datetime.now().date(),
                           pg_connect=config.pg_warehouse_db(),
                           schema='DDS',
                           logger=log)
cdm_uploads = CdmControler(date=datetime.datetime.now().date(),
                           pg_connect=config.pg_warehouse_db(),
                           schema='CDM',
                           logger=log)

with DAG(
        dag_id="example_timetable_dag",
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
        python_callable=dds_uploads.upload_aircraft_incidents,
        op_kwargs={'table_name': 'aircraft_incidents'})
    task_weather_data = PythonOperator(
        task_id='download_weather_data',
        python_callable=dds_uploads.upload_weather_observation,
        op_kwargs={'table_name': 'weather_observation'})

# инкрементальный захват инцидентов из стейджа
dds_uploads.upload_aircraft_incidents(table_name='aircraft_incidents')
# инкрементальный захват изменений в станциях (обновление справочника со станциями)
dds_uploads.upload_weather_reference(table_name='observation_reference')
# обновление DDS.incident_station_link
dds_uploads.update_incident_station_link(table_name='incident_station_link')  # Долгий запрос так как используется cross_join
# обновление данных о погоде для станции прининительно к конкретному инциденту
#dds_uploads.upload_weather_observation(table_name='weather_observation')

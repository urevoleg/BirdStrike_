import logging
import datetime
from Airflow.modules.config import Config
from Airflow.modules.dds_loader import DdsControler
from Airflow.modules.cdm_loader import CdmControler
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
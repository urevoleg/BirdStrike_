import os
import logging
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from config import Config
from modules.stg_loader import StgController
from modules.dds_loader import DdsController
from modules.cdm_loader import CdmController

config = Config()
log = logging.getLogger(__name__)

# Create working directories if they not exists
[os.mkdir(name) for name in ["Archives", "Downloads", "Unresolved"] if name not in os.listdir()]
# Clean up working directories
try:
    [os.remove(file) for file in os.listdir(os.getcwd()) if (file.endswith('.csv'))]
    [os.remove(file) for file in os.listdir(f"{os.getcwd()}/Downloads") if (file.endswith('.csv'))]
except:
    pass

stg_loadings = StgController(date=datetime.datetime.now().date(),
                             pg_connect=config.pg_warehouse_db(),
                             schema='Stage',
                             logger=log)
dds_uploads = DdsController(date=datetime.datetime.now().date(),
                            pg_connect=config.pg_warehouse_db(),
                            schema='DDS',
                            logger=log)

with DAG(
        dag_id="Only_weather",
        tags=['BirdStrike', 'weather'],
        start_date=datetime.datetime(2018, 1, 1),
        max_active_runs=1,
        schedule="5 4 * * 3",
        catchup=False,
        default_args={
            "retries": 1,
            "retry_delay": datetime.timedelta(minutes=3)}
) as dag:

    task_weather_data = PythonOperator(
        task_id='download_weather_data',
        python_callable=stg_loadings.weather_data,
        op_kwargs={'controller': stg_loadings,
                   'start_date': datetime.datetime(year=2020, month=1, day=1),
                   'end_date': datetime.datetime(year=2020, month=12, day=31)})
    upload_weather_data = PythonOperator(
        task_id='upload_weather_data',
        python_callable=dds_uploads.upload_weather_observation,
        op_kwargs={'table_name': 'weather_observation'})
    pre_upload_weather_data = PythonOperator(
        task_id='pre_upload_weather_data',
        python_callable=dds_uploads.upload_weather_observation,
        op_kwargs={'table_name': 'weather_observation'})

    pre_upload_weather_data >> upload_weather_data >> task_weather_data

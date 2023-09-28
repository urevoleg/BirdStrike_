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
cdm_loader = CdmController(date=datetime.datetime.now().date(),
                           pg_connect=config.pg_warehouse_db(),
                           schema='CDM',
                           logger=log)

with DAG(
        dag_id="Only_incidents",
        tags=['BirdStrike'],
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
        python_callable=stg_loadings.animal_incidents_data
        )
    upload_animal_incidents = PythonOperator(
        task_id='upload_animal_incidents',
        python_callable=dds_uploads.upload_aircraft_incidents,
        op_kwargs={'table_name': 'aircraft_incidents'})
    upload_incident_station_link = PythonOperator(
        task_id='upload_incident_station_link',
        python_callable=dds_uploads.update_incident_station_link,
        op_kwargs={'table_name': 'incident_station_link'})
    pre_upload_animal_incidents = PythonOperator(
        task_id='pre_upload_animal_incidents',
        python_callable=dds_uploads.upload_aircraft_incidents,
        op_kwargs={'table_name': 'aircraft_incidents'})

pre_upload_animal_incidents >> task_animal_incidents >> upload_animal_incidents >> upload_incident_station_link

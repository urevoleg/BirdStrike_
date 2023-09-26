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
        python_callable=stg_loadings.download_weather_station_reference,
        op_kwargs={'table_name': 'observation_reference'
                   })

    task_animal_incidents = PythonOperator(
        task_id='download_animal_incidents',
        python_callable=stg_loadings.animal_incidents_data,
        op_kwargs={'controller': stg_loadings,
                   })
    task_weather_data = PythonOperator(
        task_id='download_weather_data',
        python_callable=stg_loadings.weather_data,
        op_kwargs={'controller': stg_loadings,
                   'start_date': datetime.datetime(year=2018, month=1, day=1),
                   'end_date': datetime.datetime(year=2018, month=12, day=31)})
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
    task_top_airports = PythonOperator(
        task_id='top_airports',
        python_callable=cdm_loader.top_airports,
        op_kwargs={'process_date': datetime.datetime.now().date()})
    task_final_view = PythonOperator(
        task_id='final_view',
        python_callable=cdm_loader.final_view
    )
    task_save_top_airports = PythonOperator(
        task_id='top_airports_csv',
        python_callable=cdm_loader.top_airports_csv
    )

task_observation_reference >> upload_weather_reference >> upload_incident_station_link
task_animal_incidents >> upload_animal_incidents >> upload_incident_station_link
upload_incident_station_link >> task_weather_data >> upload_weather_data
upload_weather_data >> task_final_view
upload_weather_data >> task_top_airports >> task_save_top_airports

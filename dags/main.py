import os
import logging
import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from config import Config
from modules.stg_loader import StgControler
from modules.dds_loader import DdsControler
from modules.cdm_loader import CdmControler

config = Config()
log = logging.getLogger(__name__)

# Create working directories if they not exists
[os.mkdir(name) for name in ["Archives", "Downloads", "Unresolved"] if name not in os.listdir()]
# Clean up working directories
[os.remove(file) for file in os.listdir(os.getcwd()) if (file.endswith('.csv'))]
[os.remove(file) for file in os.listdir(f"{os.getcwd()}/Downloads") if (file.endswith('.csv'))]


def weather_data(controller: StgControler,
                 start_date: datetime,
                 end_date: datetime) -> None:
    """
    Function finds out incidents in DDS.aircraft_incidents,
    which doesn't have weather records in DDS.weather_observation.
    With controller methods load data from website and insert in database

    :param controller: object connector to stage schema
    :param start_date: begging date of selection
    :param end_date: end date of selection
    :return: None
    """
    with controller.pg_connect.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"""TRUNCATE TABLE STAGE.weather_observation""")  # Clean up stage table
        log.info(f"Total start_date = {start_date}")
        log.info(f"Total end_date = {end_date}")
        query = f"""
            SELECT DISTINCT indx_nr, incident_date, time, weather_station
            FROM DDS.aircraft_incidents
            INNER JOIN DDS.incident_station_link link ON aircraft_incidents.indx_nr=link.index_incedent
            WHERE incident_date between '{start_date.date()}' and '{end_date.date()}'
            AND indx_nr not in (SELECT distinct cast(incident as int)
                                FROM DDS.weather_observation)
            ORDER BY incident_date ASC"""
        cursor.execute(query)
        records = cursor.fetchall()
        log.info(f'Number of incidents, whose dont have weather data: {len(set(records))}')

    # API can receive only 50 stations at once, so task take batch with for 50 incidents at once
    for i in range(len(records[:50])):
        try:
            min_date = min([x[1] for x in records[:50]])
            log.info(f'Minimal date in batch: {min_date}')
            max_date = max([x[1] for x in records[:50]])
            log.info(f'Maximum date in batch: {max_date}')
            stations = [x[3] for x in records[:50]]

            controller.receive_weatherstation_data(stations_id=','.join(list(set(stations[:50]))),
                                                   start_datetime=min_date - datetime.timedelta(hours=1),
                                                   end_datetime=max_date + datetime.timedelta(hours=1))
            controller.load_weatherstation_data(table_name="weather_observation")
        except ValueError as e:
            log.warning(e)
        records = records[50:]


def animal_incidents_data(controller: StgControler,
                          start_date: str = None,
                          end_date: str = None) -> None:
    """
    Function with controller method receives incidents data from website and load then into database

    :param controller: object connector to stage schema
    :param start_date: begging date of selection
    :param end_date: end date of selection
    :return: None
    """
    controller.receive_animal_incidents_data(start_date=start_date, end_date=end_date)
    controller.unzip_data()
    controller.download_incidents(table_name='aircraft_incidents')


def final_view(controller: CdmControler):
    with controller.pg_connect.connection() as connect:
        cursor = connect.cursor()
        query = f"""
                CREATE OR REPLACE VIEW final AS
                SELECT indx_nr as incident, incident_date, time, EXTRACT ('YEAR' FROM incident_date) as year, 
                       species,
                       wnd, cig, vis, tmp, dew, slp
                FROM DDS.aircraft_incidents inc
                LEFT JOIN DDS.weather_observation weather ON cast(weather.incident as int) = inc.indx_nr
                """
        cursor.execute(query)
        sql_query = pd.read_sql_query('''
                                   SELECT
                                    incident, incident_date, time, year, species, wnd, cig, vis, tmp, dew, slp
                                   FROM final
                                   ''', connect)

        df = pd.DataFrame(sql_query, columns=['incident', 'incident_date', 'incident_time', 'year', 'species', 'wnd', 'cig', 'vis', 'tmp', 'dew', 'slp'])
        df.to_csv(f"{os.getcwd()}/Downloads/Final.csv", sep='\t')
        log.info(f"Final df loaded to {os.getcwd()}/Downloads/Final.csv")


def top_airports(controller: CdmControler, process_date):
    with controller.pg_connect.connection() as connect:
        cursor = connect.cursor()
        query = """
            with cte as(
            SELECT airport_id, airport, count(*), now()::date as processed_dt FROM DDS.aircraft_incidents
            GROUP BY airport_id, airport ORDER BY 3 DESC LIMIT 11) -- 11 из-за UNKNOWN но стоит обсудить с аналитиками
            SELECT cte.airport_id, airport, bts_name FROM cte
            INNER JOIN DDS.airport_bts_name --UNKNOWN на INNER JOIN отфильтруется
                ON cte.airport_id = airport_bts_name.id;
        """
        cursor.execute(query)
        tuples_airports = [(x[0], x[1], x[2]) for x in cursor.fetchall()]
        controller.top_airports_traffic(table_name='top_ten_airports', airports_data=tuples_airports,
                                        process_date=process_date)

def top_airports_csv(controller: CdmControler):
    with controller.pg_connect.connection() as connect:
        sql_query = """
                        SELECT airport_id, airport_name, year, month,
                               origin_domestic, origin_international, origin_total,
                               destination_domestic, destination_international, destination_total, report_dt
                         FROM  CDM.top_ten_airports
                        ;
                    """
        dat = pd.read_sql_query(sql_query, connect)
        df = pd.DataFrame(dat,
                          columns=['airport_id', 'airport_name', 'year', 'month', 'origin_domestic', 'origin_international',
                                   'origin_total', 'destination_domestic', 'destination_international',
                                   'destination_total', 'destination_total', 'report_dt'])
        df.to_csv(f"{os.getcwd()}/Downloads/Airports.csv", sep='\t')
        log.info(f"Airports df loaded to {os.getcwd()}/Downloads/Airports.csv")


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
                   })
    task_weather_data = PythonOperator(
        task_id='download_weather_data',
        python_callable=weather_data,
        op_kwargs={'controller': stg_loadings,
                   'start_date': datetime.datetime(year=2020, month=1, day=1),
                   'end_date': datetime.datetime(year=2020, month=12, day=31)})
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
        python_callable=top_airports,
        op_kwargs={'controller': cdm_loader,
                   'process_date': datetime.datetime.now().date()})
    task_final_view = PythonOperator(
        task_id='final_view',
        python_callable=final_view,
        op_kwargs={'controller': cdm_loader}
                   )
    task_save_top_airports = PythonOperator(
        task_id='top_airports_csv',
        python_callable=top_airports_csv,
        op_kwargs={'controller': cdm_loader}
    )


task_observation_reference >> upload_weather_reference >> upload_incident_station_link
task_animal_incidents >> upload_animal_incidents >> upload_incident_station_link
upload_incident_station_link >> task_weather_data >> upload_weather_data
upload_weather_data >> task_final_view
upload_weather_data >> task_top_airports >> task_save_top_airports

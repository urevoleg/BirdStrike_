import os
import logging
import datetime
import pandas.io.sql as sqlio
import pandas as pd

from config import Config
from modules.cdm_loader import CdmControler
from airflow import DAG
from airflow.operators.python import PythonOperator
config = Config()
log = logging.getLogger(__name__)


def final_view(controller: CdmControler):
    with controller.pg_connect.connection() as connect:
        cursor = connect.cursor()
        query = f"""
                CREATE OR REPLACE VIEW final AS
                SELECT indx_nr as incident, incident_date, incident_time, EXTRACT ('YEAR' FROM incident_date) as year, 
                       species,
                       wnd, cig, vis, tmp, dew, slp
                FROM DDS.aircraft_incidents inc
                LEFT JOIN DDS.weather_observation weather ON weather.incident = inc.indx_nr
                """
        cursor.execute(query)
        sql_query = pd.read_sql_query('''
                                   SELECT
                                    incident, incident_date, incident_time, year, species, wnd, cig, vis, tmp, dew, slp
                                   FROM final
                                   ''', connect)

        df = pd.DataFrame(sql_query, columns=['incident', 'incident_date', 'incident_time', 'year', 'species', 'wnd', 'cig', 'vis', 'tmp', 'dew', 'slp'])
        print(df)
        df.to_csv('Weather.csv', sep='\t')


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
        print(df)
        df.to_csv('Airports.csv', sep='\t')




cdm_loader = CdmControler(date=datetime.datetime.now().date(),
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
            "retry_delay": datetime.timedelta(minutes=3)}) as dag:

    task_top_airports = PythonOperator(
         task_id='download_weather_data',
         python_callable=top_airports,
         op_kwargs={'controller': cdm_loader,
                    'process_date': datetime.datetime.now().date()})


#top_airports(controller=cdm_loader, process_date=datetime.datetime.now().date())
final_view(controller=cdm_loader)

# task_top_airports
top_airports_csv(controller=cdm_loader)
import os
import pandas as pd
from .connections import PgConnect
from selenium import webdriver
import time
import datetime
from selenium.webdriver.common.by import By
from .instruments import table_extractor


class CdmController:
    def __init__(self, date: datetime.date,
                 pg_connect: PgConnect,
                 schema: str,
                 logger):
        self.date = date
        self.pg_connect = pg_connect
        self.logger = logger
        self.schema = schema

    def top_airports_traffic(self, table_name, airports_data: list, process_date: datetime.date):
        url = "https://www.transtats.bts.gov/Data_Elements.aspx"
        options = webdriver.ChromeOptions()
        options.add_argument('headless')
        remote_webdriver = 'remote_chromedriver'
        with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
            driver.get(url)
            driver.set_window_size(1920, 1080)  # Без этой опции не подгружается кнопка submit в фоновом режиме
            time.sleep(5)
            driver.find_element(By.ID, 'Link_Flights').click()
            time.sleep(5)
            with self.pg_connect.connection() as connect:
                connect.autocommit = False
                for record in airports_data:
                    airport_id = record[0]
                    airport_name = record[1]
                    bts_name = record[2]
                    driver.find_element(By.ID, 'AirportList').send_keys(bts_name)
                    driver.find_element(By.ID, 'Link_Origin').click()
                    time.sleep(5)
                    driver.find_element(By.NAME, 'Submit').click()
                    time.sleep(5)
                    html = driver.page_source
                    origin_dataframe = table_extractor(html=html)
                    origin_dataframe.rename(
                        columns={'DOMESTIC': 'origin_domestic', 'INTERNATIONAL': 'origin_international',
                                 'TOTAL': 'origin_total'}, inplace=True)
                    driver.find_element(By.ID, 'Link_Destination').click()
                    driver.find_element(By.NAME, 'Submit').click()
                    html = driver.page_source
                    destination_dataframe = table_extractor(html=html)
                    destination_dataframe.rename(columns={'DOMESTIC': 'destination_domestic',
                                                          'INTERNATIONAL': 'destination_international',
                                                          'TOTAL': 'destination_total'}, inplace=True)
                    result = origin_dataframe.merge(destination_dataframe, on=["Year", "Month"])
                    self.logger.info(f"Подготовлен Dataframe c {result.shape[0]} записями для {bts_name}")
                    columns = ['airport_id', 'airport_name', 'Year', 'Month', 'origin_domestic', 'origin_international',
                               'origin_total', 'destination_domestic', 'destination_international', 'destination_total',
                               'report_dt']

                    cursor = connect.cursor()
                    for row in result.itertuples():
                        query = f"""
                                                    INSERT INTO {self.schema}.{table_name} ({" ,".join(columns)}) VALUES
                                                    ('{airport_id}', '{airport_name}', '{row.Year}',
                                                        '{row.Month}', '{row.origin_domestic}', 
                                                        '{row.origin_international}',
                                                        '{row.origin_total}', '{row.destination_domestic}', 
                                                        '{row.destination_international}', 
                                                        '{row.destination_total}', '{process_date}'::date
                                                        );"""
                        try:
                            cursor.execute(query)
                        except Exception as e:
                            self.logger.error(e)
                            print(e)
                            self.logger.info(e)
                    connect.commit()
            self.logger.info(f"All data loaded to {self.schema}.{table_name}")

    def top_airports_csv(self):
        with self.pg_connect.connection() as connect:
            sql_query = """
                            SELECT airport_id, airport_name, year, month,
                                   origin_domestic, origin_international, origin_total,
                                   destination_domestic, destination_international, destination_total, report_dt
                             FROM  CDM.top_ten_airports
                            ;
                        """
            data = pd.read_sql_query(sql_query, connect)
            df = pd.DataFrame(data,
                              columns=['airport_id', 'airport_name', 'year', 'month', 'origin_domestic',
                                       'origin_international',
                                       'origin_total', 'destination_domestic', 'destination_international',
                                       'destination_total', 'destination_total', 'report_dt'])
            df.to_csv(f"{os.getcwd()}/Downloads/Airports.csv", sep='\t')
            self.logger.info(f"Airports df loaded to {os.getcwd()}/Downloads/Airports.csv")

    def top_airports(self, process_date):
        """
        Method insert data about top 10 airports by incidents with birdstrike into top_ten_airports table
        :param process_date: date when data loaded into top_ten_airports
        :return:
        """
        with self.pg_connect.connection() as connect:
            cursor = connect.cursor()
            query = """
                with cte as(
                SELECT airport_id, airport, count(*), now()::date as processed_dt FROM DDS.aircraft_incidents
                GROUP BY airport_id, airport ORDER BY 3 DESC LIMIT 11) -- 11 из-за UNKNOWN но стоит обсудить с аналитиками
                SELECT cte.airport_id, airport, bts_name FROM cte
                INNER JOIN DDS.airport_bts_name --UNKNOWN with INNER JOIN will be filtered
                    ON cte.airport_id = airport_bts_name.id;
            """
            cursor.execute(query)
            tuples_airports = [(x[0], x[1], x[2]) for x in cursor.fetchall()]
            self.top_airports_traffic(table_name='top_ten_airports', airports_data=tuples_airports,
                                      process_date=process_date)

    def final_view(self):
        """
        Method creates result view and save it in csv
        ::return: None
        """
        with self.pg_connect.connection() as connect:
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

            df = pd.DataFrame(sql_query,
                              columns=['incident', 'incident_date', 'incident_time', 'year', 'species', 'wnd', 'cig',
                                       'vis',
                                       'tmp', 'dew', 'slp'])
            df.to_csv(f"{os.getcwd()}/Downloads/Final.csv", sep='\t')
            self.logger.info(f"Final df loaded to {os.getcwd()}/Downloads/Final.csv")

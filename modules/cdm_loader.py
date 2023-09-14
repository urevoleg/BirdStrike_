from modules.connections import PgConnect
import requests as req
from selenium import webdriver
import time
import datetime
from selenium.webdriver.common.by import By
from modules.instrumentals import table_extractor


class CdmControler:
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
        response = req.get(url)
        if response.status_code == 200:
            print(f"Страница доступна")
            self.logger.info(f"Страница доступна")
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            driver = webdriver.Chrome(options=options)  # Открытие страницы в фоновом режиме
            driver.get(url)
            driver.set_window_size(1920, 1080)  # Без этой опции не подгружается кнопка submit в фоновом режиме
            time.sleep(5)
            driver.find_element(By.ID, 'Link_Flights').click()
            time.sleep(5)
            with self.pg_connect.connection() as connect:
                connect.autocommit = False
                for record in airports_data:
                    print(record)
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
                    print(f"All data loaded to {self.schema}.{table_name}")
                    self.logger.info(f"All data loaded to {self.schema}.{table_name}")

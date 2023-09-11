import os
import shutil
import time
import zipfile
import requests as req
from datetime import datetime
from selenium.webdriver.common.by import By
import pandas as pd
from .instrumentals import clean_directory, table_extractor
from .connections import PgConnect
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager


class StgControler:
    def __init__(self, date: datetime.date,
                 pg_connect: PgConnect,
                 schema: str,
                 logger):
        self.date = date
        self.pg_connect = pg_connect
        self.downloaded_files_list = []
        self.result_files_list = []
        self.logger = logger
        self.schema = schema

    def unzip_data(self):
        for archive in self.downloaded_files_list:
            if archive.endswith('.zip'):
                with zipfile.ZipFile(f"{os.getcwd()}/Downloads/{archive}") as archive_file:
                    files_list = archive_file.namelist()
                    for file in files_list:
                        archive_file.extract(member=file, path=f"{os.getcwd()}/Archives/{self.date}")
                        self.logger.info(msg=f"File {file} extracted in {os.getcwd()}/Archives/{self.date}")
                        print(f"File {file} extracted in {os.getcwd()}/Archives/{self.date}")
                        self.result_files_list.append(f"{os.getcwd()}/Archives/{self.date}/{file}")

    def download_incidents(self, table_name):
        for file_name in self.result_files_list:
            df = pd.read_excel(io=file_name, sheet_name='data')
            with self.pg_connect.connection() as connect:
                connect.autocommit = False
                result_df = df[['INDX_NR', 'INCIDENT_DATE ', 'LATITUDE', 'LONGITUDE', 'AIRPORT', 'AIRPORT_ID', 'SPECIES']]
                result_df.rename(columns={'INCIDENT_DATE ': 'INCIDENT_DATE'}, inplace=True)
                result_df = result_df.astype({'LATITUDE': str, 'LONGITUDE': str})  # очень сильно замедляет работу, но это вынужденный шаг из-за одной записи в 2022 году
                self.logger.info(f"Обрабатывается Dataframe c {result_df.shape[0]} записями")
                print(f"Обрабатывается Dataframe c {result_df.shape[0]} записями")
                columns = 'INDX_NR, INCIDENT_DATE, LATITUDE, LONGITUDE, AIRPORT, AIRPORT_ID, SPECIES'
                cursor = connect.cursor()
                unloaded_rows = []
                for row in result_df.itertuples():
                    request_all_indexes = """SELECT indx_nr FROM DDS.aircraft_incidents"""
                    try:
                        query = f"""
                                INSERT INTO {self.schema}.{table_name} ({columns})
                                with cte as(
                                SELECT 
                                '{row.INDX_NR}' as INDX_NR, '{row.INCIDENT_DATE}' as INCIDENT_DATE, 
                                '{row.LATITUDE.replace("°", '').replace("'", '').replace("?N", '0').replace("?S", '0')}' as LATITUDE, -- одна запись с таким форматом: "49°09'05?N" - костыль, но вспоминать регулярки это время 
                                '{row.LONGITUDE.replace("°", '').replace("'", '').replace("?E", '0').replace("?W", '0')}' as LONGITUDE, -- одна запись с таким форматом: 16°41'40?E - костыль, но вспоминать регулярки это время
                                '{row.AIRPORT.replace("'", '')}' as AIRPORT, 
                                '{row.AIRPORT_ID}' as AIRPORT_ID, 
                                '{row.SPECIES.replace("'", '')}' as SPECIES)
                                SELECT INDX_NR, INCIDENT_DATE, LATITUDE, LONGITUDE, AIRPORT, AIRPORT_ID, SPECIES
                                FROM cte
                                WHERE INDX_NR NOT IN ({request_all_indexes});"""

                        cursor.execute(query)
                    except Exception as e:
                        self.logger.error(e)
                        unloaded_rows.append(row)
                        pass
                connect.commit()
                if len(unloaded_rows) > 0:
                    self.logger.info(
                        f"{len(unloaded_rows)} записей не были загружены в таблицу {self.schema}.{table_name}")
                    for record in unloaded_rows:
                        self.logger.warning(record)
                print('Data loaded')

    def load_weatherstation_data(self, table_name, station_id):

        with self.pg_connect.connection() as connect:
            connect.autocommit = False
            cursor = connect.cursor()
            columns = "STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, TEMP, TEMP_ATTRIBUTES, DEWP, DEWP_ATTRIBUTES, " \
                      "SLP, SLP_ATTRIBUTES ,STP, STP_ATTRIBUTES, VISIB, VISIB_ATTRIBUTES, WDSP, WDSP_ATTRIBUTES, MXSPD, GUST, " \
                      "MAX, MAX_ATTRIBUTES, MIN, MIN_ATTRIBUTES, PRCP, PRCP_ATTRIBUTES, SNDP, FRSHTT "

            if f"{station_id}.csv" in self.downloaded_files_list:
                df = pd.read_csv(f"{os.getcwd()}/Downloads/{station_id}.csv")
                print(f"Подготовлен Dataframe c {df.shape[0]} записями")
                unloaded_rows = []
                for row in df.itertuples():

                    query = f"""
                                INSERT INTO {self.schema}.{table_name} ({columns}) VALUES
                                ('{row.STATION}', '{row.DATE}', '{row.LATITUDE}',
                                    '{row.LONGITUDE}', '{row.ELEVATION}', '{row.NAME}', 
                                    '{row.TEMP}', '{row.TEMP_ATTRIBUTES}', '{row.DEWP}', '{row.DEWP_ATTRIBUTES}', 
                                    '{row.SLP}', '{row.SLP_ATTRIBUTES}', '{row.STP}', '{row.STP_ATTRIBUTES}', '{row.VISIB}', 
                                    '{row.VISIB_ATTRIBUTES}', '{row.WDSP}', '{row.WDSP_ATTRIBUTES}', '{row.MXSPD}', 
                                    '{row.GUST}', '{row.MAX}', '{row.MAX_ATTRIBUTES}', '{row.MIN}', '{row.MIN_ATTRIBUTES}', 
                                    '{row.PRCP}', '{row.PRCP_ATTRIBUTES}', '{row.SNDP}', '{row.FRSHTT}'
                                    );"""
                    try:
                        cursor.execute(query)
                    except Exception as e:
                        self.logger.error(e)
                        unloaded_rows.append(row)
                connect.commit()
                if len(unloaded_rows) == 0:
                    os.remove(f"{os.getcwd()}/Downloads/{station_id}.csv")
                    self.logger.info(f"{os.getcwd()}/Downloads/{station_id}.csv удален")
                    print(f"{os.getcwd()}/Downloads/{station_id}.csv удален")
                else:
                    self.logger.info(
                        f"{len(unloaded_rows)} записей не были загружены в таблицу {self.schema}.{table_name}:")
                    for record in unloaded_rows:
                        self.logger.warning(record)
                    shutil.move(f"{os.getcwd()}/Downloads/{station_id}.csv",
                                f"{os.getcwd()}/Unresolved/{station_id}.csv")

    def receive_weatherstation_data(self,
                                    station_id: str,
                                    year: int
                                    ):
        """

            :param year:
            :param station_id:
            :return:
            """

        download_url = f"""https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year}/{station_id}.csv"""
        clean_directory(full_path=f"{os.getcwd()}/{station_id}")  # Зачищает папку от файлов прежних неуспешных запусков
        response = req.get(download_url)
        if response.status_code == 200:
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            driver = webdriver.Chrome(options=options)
            driver.get(download_url)
            time.sleep(5)
            current_file = [file for file in os.listdir(f"{os.getcwd()}") if file.startswith(f'{station_id}')][0]
            print(f"{os.getcwd()}/Downloads/{station_id}")
            clean_directory(full_path=f"{os.getcwd()}/Downloads/{station_id}.csv")  # Зачищает целевую папку файла
            shutil.move(src=f"{os.getcwd()}/{current_file}", dst=f"{os.getcwd()}/Downloads", )
            time.sleep(5)
            print(f"file {current_file} moved to {os.getcwd()}/Downloads")
            self.logger.info(f"file {current_file} moved to {os.getcwd()}/Downloads")
            self.downloaded_files_list.append(current_file)

    def receive_animal_incidents_data(self,
                                      start_date: str,
                                      end_date: str
                                      ):
        """

            :param start_date:
            :param end_date:
            :return:
            """
        url = f"""https://wildlife.faa.gov/search"""  # роутер перебрасыват на страницу home, поэтому переход придется выполнять вручную
        response = req.get(url)
        # Зачищаем папку от zip файлов
        [clean_directory(full_path=f"{os.getcwd()}/{file}")
         for file in os.listdir(f"{os.getcwd()}") if file.endswith('.zip')]

        if response.status_code == 200:
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            driver = webdriver.Chrome(options=options)  # Открытие страницы в фоновом режиме
            # driver = webdriver.Chrome(ChromeDriverManager().install())
            # driver = webdriver.Chrome()
            driver.get(url)
            driver.find_element(By.CSS_SELECTOR,
                                '#body > app-home > div > mat-card > mat-card-content > div > div > div.row > '
                                'div:nth-child(1) > a').click()
            time.sleep(5)
            driver.find_element(By.NAME, 'fromDate').send_keys(start_date)
            driver.find_element(By.NAME, 'toDate').send_keys(end_date)
            driver.find_element(By.XPATH,
                                '//*[@id="body"]/app-search/div[1]/mat-card/mat-card-content/div/div[1]/div[2]/div['
                                '2]/div[2]/span[1]/button[1]/span[2]').click()
            print(f"Page {url} opened and filed, 10 seconds wait until data will be prepared")
            self.logger.info(f"Page {url} opened and filed, 10 seconds wait until data will be prepared")
            time.sleep(10)
            for i in range(5):
                try:
                    driver.find_element(By.CSS_SELECTOR,
                                        '#body > app-search > div.content > mat-card > mat-card-content > div > '
                                        'div.card.airport-information > div.card-body > '
                                        'div.card-footer.remove-margin.row > div.col-md-6.text-right.float-right > '
                                        'span:nth-child(2)').click()
                    break
                except:
                    print(f"Attempt № {i} failed")
                    self.logger.warning(f"Attempt № {i} failed")
            time.sleep(60)  # Время для скачивания файла с данными за 1 год запасом
            current_file = [file for file in os.listdir(f"{os.getcwd()}") if file.endswith('.zip')][0]
            clean_directory(full_path=f"{os.getcwd()}/Downloads/{current_file}")  # Зачищает целевую папку
            shutil.move(src=f"{os.getcwd()}/{current_file}", dst=f"{os.getcwd()}/Downloads", )
            print(f"file {current_file} moved to {os.getcwd()}/Downloads")
            self.logger.info(f"file {current_file} moved to {os.getcwd()}/Downloads")
            self.downloaded_files_list.append(current_file)

    def top_airports_traffic(self, table_name, airports_data: list, process_date: datetime.date):
        url = "https://www.transtats.bts.gov/Data_Elements.aspx"
        response = req.get(url, verify=False)
        if response.status_code == 200:
            print(f"Страница доступна")
            self.logger.info(f"Страница доступна")
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service as ChromeService
            service = ChromeService(executable_path='/usr/local/bin/chromedriver', service_log_path='/opt/airflow/logs')
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            driver = webdriver.Chrome(service=service, options=options,)

            # driver = webdriver.Chrome()
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
                    print(f"Подготовлен Dataframe c {result.shape[0]} записями для {bts_name}")
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
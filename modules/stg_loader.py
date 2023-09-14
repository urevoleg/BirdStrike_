import os
import shutil
import time
import zipfile
from selenium import webdriver
import requests as req
import datetime
from selenium.webdriver.common.by import By
import pandas as pd
from modules.instrumentals import clean_directory
from modules.connections import PgConnect


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

    def isd_history(self, table_name: str, begining_date: str = '2018-01-01') -> None:
        """
            Метод загрузки всех справочных данных о станциях

        :param table_name: Наименование таблицы для сохранения
        :param begining_date: Начальная дата загрузки
        :return: None
        """
        download_url = f"""https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"""
        response = req.get(download_url)
        [clean_directory(full_path=f"{os.getcwd()}/{file}") for file in os.listdir(f"{os.getcwd()}") if
         file.startswith('isd')]
        if response.status_code == 200:
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            driver = webdriver.Chrome(options=options)
            driver.get(download_url)
            time.sleep(20)  # большой файл, нужно время на загрузку
            isd_file = [file for file in os.listdir(f"{os.getcwd()}") if file.startswith('isd-history.')][0]
            df = pd.read_csv(filepath_or_buffer=isd_file, engine='python', encoding='utf-8', on_bad_lines='warn')
            df['station'] = df['USAF'].astype(str) + df["WBAN"].astype(str)
            result_df = df.query(f"`END` >= {begining_date.replace('-', '')}").query("`CTRY` == 'US'")
            result_df = result_df[['station', 'BEGIN', 'END', 'LAT', 'LON']]
            result_df.rename(columns={'BEGIN': 'start_date', 'END': 'end_date'}, inplace=True)
            columns = ['station', 'start_date', 'end_date', 'GEO_DATA']
            self.logger.info(f'Dataframe has {result_df.shape[0]} rows')
            if not result_df.shape[0] > 0:
                self.logger.info(f'Dataframe is empty')
            else:
                with self.pg_connect.connection() as connect:
                    connect.autocommit = False
                    cursor = connect.cursor()
                    cursor.execute(f"TRUNCATE TABLE {self.schema}.{table_name};")
                    for row in result_df.itertuples():
                        try:
                            query = f"""
                                    INSERT INTO {self.schema}.{table_name} ({','.join(columns)})
                                    with cte as(
                                    SELECT
                                    '{row.station}' as station, 
                                    {int(row.start_date)} as start_date,
                                    {int(row.end_date)} as end_date,
                                    point({row.LAT}, {row.LON}) as GEO_DATA
                                    )
                                    SELECT station, start_date, end_date, GEO_DATA
                                    FROM cte;"""
                            cursor.execute(query)

                        except Exception as e:
                            self.logger.error(e)
                            self.logger.info(row)
                            pass
                    connect.commit()
                self.logger.info(f'Data loaded to {self.schema}.{table_name}')
            clean_directory(full_path=f"{os.getcwd()}/{isd_file}")

    def unzip_data(self) -> None:
        """
        Метод разархивирует файлы в папке Downloads
        и сохраняет их наименования в result_files_list

        :return: None
        """
        for archive in self.downloaded_files_list:
            if archive.endswith('.zip'):
                with zipfile.ZipFile(f"{os.getcwd()}/Downloads/{archive}") as archive_file:
                    files_list = archive_file.namelist()
                    for file in files_list:
                        archive_file.extract(member=file, path=f"{os.getcwd()}/Archives/{self.date}")
                        self.logger.info(msg=f"File {file} extracted in {os.getcwd()}/Archives/{self.date}")
                        self.result_files_list.append(f"{os.getcwd()}/Archives/{self.date}/{file}")

    def download_incidents(self, table_name: str) -> None:
        """
        Метод обрабатывает файлы в параметре result_files_list и загружает их в слой сырых данных

        :param table_name: наименование таблицы в слое сырых данных
        :return: None
        """
        for file_name in self.result_files_list:
            df = pd.read_excel(io=file_name, sheet_name='data')
            with self.pg_connect.connection() as connect:
                connect.autocommit = False
                df['LATITUDE'] = df['LATITUDE'].fillna('0')
                df['LONGITUDE'] = df['LONGITUDE'].fillna('0')
                result_df = df[
                    ['INDX_NR', 'INCIDENT_DATE ', 'TIME', 'LATITUDE', 'LONGITUDE', 'AIRPORT', 'AIRPORT_ID', 'SPECIES']]
                result_df.rename(columns={'INCIDENT_DATE ': 'INCIDENT_DATE'}, inplace=True)
                # очень сильно замедляет работу, но это вынужденный шаг из-за одной записи в 2022 году
                result_df = result_df.astype({'LATITUDE': str,
                                              'LONGITUDE': str})
                self.logger.info(f"Обрабатывается Dataframe c {result_df.shape[0]} записями")
                columns = 'INDX_NR, INCIDENT_DATE, inc_coordinates, AIRPORT, AIRPORT_ID, SPECIES, TIME'

                cursor = connect.cursor()
                unloaded_rows = []
                for row in result_df.itertuples():
                    request_all_indexes = """SELECT indx_nr FROM DDS.aircraft_incidents"""
                    query = f"""
                        INSERT INTO {self.schema}.{table_name} ({columns})
                        with cte as(
                        SELECT 
                        '{row.INDX_NR}' as INDX_NR, '{row.INCIDENT_DATE}' as INCIDENT_DATE, 
                        point({row.LATITUDE.replace('°', '').replace("'", '').replace("?N", '').replace("?S", '')}, 
                              {row.LONGITUDE.replace('°', '').replace("'", '').replace("?E", '').replace("?W", '')}) 
                              as inc_coordinates,
                        '{row.AIRPORT.replace("'", '')}' as AIRPORT, 
                        '{row.AIRPORT_ID}' as AIRPORT_ID, 
                        '{row.SPECIES.replace("'", '')}' as SPECIES,
                        '{row.TIME}' as time)
                        SELECT INDX_NR, INCIDENT_DATE, inc_coordinates, AIRPORT, AIRPORT_ID, SPECIES, time
                        FROM cte
                        WHERE INDX_NR NOT IN ({request_all_indexes}) ON CONFLICT DO NOTHING;"""
                    try:
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
                self.logger.info(f"Data loaded")

    def receive_animal_incidents_data(self,
                                      start_date: str = None,
                                      end_date: str = None,
                                      history_start_date: str = '2018-01-01'
                                      ) -> None:
        """
            Метод принимает начальную и конечную дату для расчета.
            Если параметры не переданы, то расчитываются дефолтные параметры за 4 недели
            Метод обращается к сайту с данными, заполняет графы и скачивает файл с инцидентами за указанный промежуток времени
            Файл переносится в папку Downloads, а его имя сохраняется в self.downloaded_files_list

            :param history_start_date: минимальная дефолтная дата загрузки данных
            :param start_date: начало периода выборки данных
            :param end_date: конец периода выборки данных
            :return: None
            """
        if start_date is None:
            with self.pg_connect.connection() as connect:
                cursor = connect.cursor()
                cursor.execute(f"""SELECT max(incident_date) FROM DDS.aircraft_incidents;""")
                start_date = cursor.fetchone()[0]
                try:
                    start_date = datetime.datetime.strftime(start_date, '%Y-%m-%d')
                except TypeError:
                    start_date = history_start_date

        if end_date is None:
            end_date = datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(weeks=26)
            end_date = datetime.datetime.strftime(end_date, '%Y-%m-%d')
        days_difference = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date,
                                                                                                        '%Y-%m-%d')
        url = f"""https://wildlife.faa.gov/search"""  # роутер перебрасыват на страницу home, поэтому переход придется выполнять вручную
        response = req.get(url)

        # Зачищаем папку от zip файлов
        [clean_directory(full_path=f"{os.getcwd()}/{file}")
         for file in os.listdir(f"{os.getcwd()}") if file.endswith('.zip')]
        self.logger.info(f"Atempt to find data between {start_date} and {end_date}. Range {days_difference.days} days")
        if response.status_code == 200:
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            driver = webdriver.Chrome(options=options)  # Открытие страницы в фоновом режиме
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
            for i in range(1, 5):  # 5 попыток нажать на кнопку скачивания файла
                try:
                    driver.find_element(By.CSS_SELECTOR,
                                        '#body > app-search > div.content > mat-card > mat-card-content > div > '
                                        'div.card.airport-information > div.card-body > '
                                        'div.card-footer.remove-margin.row > div.col-md-6.text-right.float-right > '
                                        'span:nth-child(2)').click()
                    break
                except Exception:
                    print(f"Attempt № {i} failed")
                    self.logger.warning(f"Attempt № {i} failed")
            time.sleep(days_difference.days / 2)  # Время для скачивания файла из расчета 0,5 секунды на загрузку 1 дня
            current_file = [file for file in os.listdir(f"{os.getcwd()}") if file.endswith('.zip')][0]
            # Зачищаем целевую папку
            clean_directory(full_path=f"{os.getcwd()}/Downloads/{current_file}")
            shutil.move(src=f"{os.getcwd()}/{current_file}", dst=f"{os.getcwd()}/Downloads", )
            self.logger.info(f"file {current_file} moved to {os.getcwd()}/Downloads")
            self.downloaded_files_list.append(current_file)

    def load_weatherstation_data(self, table_name):
        "ПОКА под вопросом"
        with self.pg_connect.connection() as connect:
            connect.autocommit = False
            cursor = connect.cursor()
            columns = "STATION, DATE, WND, CIG, VIS, TMP, DEW, SLP"
            for file in self.downloaded_files_list:
                df = pd.read_csv(f"{os.getcwd()}/Downloads/{file}")
                print(f"Подготовлен Dataframe c {df.shape[0]} записями")
                unloaded_rows = []
                query = f"""
                        INSERT INTO {self.schema}.{table_name} ({columns}) VALUES 
                        """
                for row in df.itertuples():
                    query += f"""('{row.STATION}', '{row.DATE}', '{row.WND}', '{row.CIG}', '{row.VIS}', '{row.TMP}', 
                    '{row.DEW}', '{row.SLP}'),"""
                try:
                    cursor.execute(query[:-1]+';')
                except Exception as e:
                    self.logger.error(e)
                    unloaded_rows.append(row)
                connect.commit()
                self.logger.info(f"Another butch with {df.shape[0]} rows downloaded")
                self.downloaded_files_list.remove(file)
                if len(unloaded_rows) == 0:
                    os.remove(f"{os.getcwd()}/Downloads/{file}")
                    self.logger.info(f"{os.getcwd()}/Downloads/{file} удален")
                else:
                    self.logger.info(
                        f"{len(unloaded_rows)} записей не были загружены в таблицу {self.schema}.{table_name}:")
                    for record in unloaded_rows:
                        self.logger.warning(record)
                    shutil.move(f"{os.getcwd()}/Downloads/{file}",
                                f"{os.getcwd()}/Unresolved/{file}")
                self.logger.info(f"Data is loaded")

    def receive_weatherstation_data(self,
                                    station_id: str,
                                    start_datetime: datetime,
                                    end_datetime: datetime
                                    ):
        """
        Пока под вопросом
            """

        URL = f"""https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly&stations={station_id}&startDate={start_datetime}T00:00:00&endDate={end_datetime}T23:59:59&includeAttributes=true&format=csv"""
        print(URL)
        clean_directory(full_path=f"{os.getcwd()}/{station_id}")  # Зачищает папку от файлов прежних неуспешных запусков
        response = req.get(URL)
        if response.status_code == 200:
            options = webdriver.ChromeOptions()
            options.add_argument('headless')
            driver = webdriver.Chrome(options=options)
            driver.get(URL)
            for i in range(8):
                try:
                    time.sleep(10)
                    current_file = [file for file in os.listdir(f"{os.getcwd()}") if file.endswith('csv')][0]
                    break
                except:
                    self.logger.warning(f"File .csv not found")
                    pass
            clean_directory(full_path=f"{os.getcwd()}/Downloads/{current_file}")  # Зачищает целевую папку файла
            shutil.move(src=f"{os.getcwd()}/{current_file}", dst=f"{os.getcwd()}/Downloads/", )
            time.sleep(10)
            self.logger.info(f"file {current_file} moved to {os.getcwd()}/Downloads")
            self.downloaded_files_list.append(current_file)

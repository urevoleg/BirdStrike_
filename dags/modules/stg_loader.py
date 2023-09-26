import os
import shutil
import time
import zipfile

import numpy as np
from selenium import webdriver
import requests as req
import datetime
from selenium.webdriver.common.by import By
import pandas as pd
from .instrumentals import clean_directory
from .connections import PgConnect


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
            os.system(f"curl -O {download_url}")
            for i in range(1, 5):
                try:
                    isd_file = [file for file in os.listdir(f"{os.getcwd()}") if file.startswith('isd-history.')][0]
                    break
                except:
                    self.logger.info(f"Attemp № {i} to find file isd-history failed")
                    time.sleep(20)
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
        self.downloaded_files_list.clear()

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
                df['COMMENTS'], df['REMARKS'] = df['COMMENTS'].astype(str), df['REMARKS'].astype(str)

                df.rename(columns={'INCIDENT_DATE ': 'INCIDENT_DATE'}, inplace=True)
                result_df = df.astype(object).replace(np.nan, 'None')
                # очень сильно замедляет работу, но это вынужденный шаг из-за одной записи в 2022 году
                result_df = result_df.astype({'LATITUDE': str,
                                              'LONGITUDE': str})

                self.logger.info(f"Обрабатывается Dataframe c {result_df.shape[0]} записями")
                columns = 'INDX_NR, INCIDENT_DATE, INCIDENT_MONTH, INCIDENT_YEAR, TIME, TIME_OF_DAY, AIRPORT_ID, ' \
                          'AIRPORT, inc_coordinates, LATITUDE, LONGITUDE, RUNWAY, STATE, FAAREGION, LOCATION, ' \
                          'ENROUTE_STATE, OPID, OPERATOR, REG, FLT, AIRCRAFT, AMA, AMO, EMA, EMO, AC_CLASS, AC_MASS, ' \
                          'TYPE_ENG, NUM_ENGS, ENG_1_POS, ENG_2_POS, ENG_3_POS, ENG_4_POS, PHASE_OF_FLIGHT, HEIGHT, ' \
                          'SPEED, DISTANCE, SKY, PRECIPITATION, AOS, COST_REPAIRS, COST_OTHER, ' \
                          'COST_REPAIRS_INFL_ADJ, COST_OTHER_INFL_ADJ, INGESTED_OTHER, INDICATED_DAMAGE, ' \
                          'DAMAGE_LEVEL, STR_RAD, DAM_RAD, STR_WINDSHLD, DAM_WINDSHLD, STR_NOSE, DAM_NOSE,' \
                          'STR_ENG1, DAM_ENG1, ING_ENG1, STR_ENG2, DAM_ENG2, ING_ENG2, STR_ENG3, DAM_ENG3, ING_ENG3, ' \
                          'STR_ENG4, DAM_ENG4, ING_ENG4, STR_PROP, DAM_PROP, STR_WING_ROT, DAM_WING_ROT, STR_FUSE,' \
                          'DAM_FUSE, STR_LG, DAM_LG, STR_TAIL, DAM_TAIL, STR_LGHTS, DAM_LGHTS, STR_OTHER, DAM_OTHER, ' \
                          'OTHER_SPECIFY, EFFECT, EFFECT_OTHER, SPECIES_ID, SPECIES, OUT_OF_RANGE_SPECIES, ' \
                          'REMARKS, REMAINS_COLLECTED, REMAINS_SENT, BIRD_BAND_NUMBER, WARNED, NUM_SEEN, NUM_STRUCK, ' \
                          'SIZE, NR_INJURIES, NR_FATALITIES, COMMENTS, REPORTER_NAME, REPORTER_TITLE, SOURCE, PERSON, '\
                          'LUPDATE, IMAGE, TRANSFER'

                cursor = connect.cursor()
                unloaded_rows = []
                for row in result_df.itertuples():
                    request_all_indexes = """SELECT indx_nr FROM DDS.aircraft_incidents"""
                    query = f"""
                        INSERT INTO {self.schema}.{table_name} ({columns})
                        with cte as(
                        SELECT 
                        '{int(row.INDX_NR)}' as INDX_NR, '{row.INCIDENT_DATE}' as INCIDENT_DATE,
                        '{int(row.INCIDENT_MONTH)}' as INCIDENT_MONTH, 
                        '{int(row.INCIDENT_YEAR)}' as INCIDENT_YEAR,
                        '{str(row.TIME or '')}' as time, 
                        '{str(row.TIME_OF_DAY or '')}' as TIME_OF_DAY, 
                        '{str(row.AIRPORT_ID or '')}' as AIRPORT_ID,
                        '{str(row.AIRPORT.replace("'", ''))}' as AIRPORT,
                        point({row.LATITUDE.replace('°', '').replace("'", '').replace("?N ", '').replace("?S", '')}, 
                              {row.LONGITUDE.replace('°', '').replace("'", '').replace("?E", '').replace("?W", '')}) 
                              as inc_coordinates,
                        '{str(row.LATITUDE).replace('°', '').replace("'", '').replace("?N ", '').replace("?S", '')}' as LATITUDE, 
                        '{str(row.LONGITUDE).replace('°', '').replace("'", '').replace("?E", '').replace("?W", '')}' as LONGITUDE,
                        '{str(row.RUNWAY or None).replace("'",'')}' as RUNWAY, 
                        '{str(row.STATE or None)}' as STATE, 
                        '{str(row.FAAREGION or None)}' as FAAREGION,
                        '{str(row.LOCATION or None).replace("'", ' ')}' as LOCATION, 
                        '{str(row.ENROUTE_STATE or None)}' as ENROUTE_STATE, 
                        '{str(row.OPID or None)}' as OPID,
                        '{str(row.OPERATOR or None)}' as OPERATOR, 
                        '{str(row.REG or None)}' as REG, 
                        '{str(row.FLT or None)}' as FLT,
                        '{str(row.AIRCRAFT or None)}' as AIRCRAFT, 
                        '{str(row.AMA or None)}' as AMA, 
                        '{str(row.AMO or None)}' as AMO, 
                        '{str(row.EMA or None)}' as EMA,
                        '{str(row.EMO or None)}' as EMO,
                        '{str(row.AC_CLASS or None)}' as AC_CLASS,
                        '{str(row.AC_MASS or None)}' as AC_MASS,
                        '{str(row.TYPE_ENG or None)}' as TYPE_ENG,
                        '{str(row.NUM_ENGS or None)}' as NUM_ENGS,
                        '{str(row.ENG_1_POS or None)}' as ENG_1_POS,
                        '{str(row.ENG_2_POS or None)}' as ENG_2_POS,
                        '{str(row.ENG_3_POS or None)}' as ENG_3_POS,
                        '{str(row.ENG_4_POS or None)}' as ENG_4_POS,
                        '{str(row.PHASE_OF_FLIGHT or None)}' as PHASE_OF_FLIGHT,
                        '{str(row.HEIGHT or None)}' as HEIGHT,
                        '{str(row.SPEED or None)}' as SPEED,
                        '{str(row.DISTANCE or None)}' as DISTANCE,
                        '{str(row.SKY or None)}' as SKY,
                        '{str(row.PRECIPITATION or None)}' as PRECIPITATION,
                        '{str(row.AOS or None)}' as AOS,
                        '{str(row.COST_REPAIRS or None)}' as COST_REPAIRS,
                        '{str(row.COST_OTHER or None)}' as COST_OTHER,
                        '{str(row.COST_REPAIRS_INFL_ADJ or None)}' as COST_REPAIRS_INFL_ADJ,
                        '{str(row.COST_OTHER_INFL_ADJ or None)}' as COST_OTHER_INFL_ADJ,
                        '{bool(row.INGESTED_OTHER or None)}' as INGESTED_OTHER,
                        '{bool(row.INDICATED_DAMAGE or None)}' as INDICATED_DAMAGE,
                        '{str(row.DAMAGE_LEVEL or None)}' as DAMAGE_LEVEL,
                        '{bool(row.STR_RAD or None)}' as STR_RAD,
                        '{bool(row.DAM_RAD or None)}' as DAM_RAD,
                        '{bool(row.STR_WINDSHLD or None)}' as STR_WINDSHLD,
                        '{bool(row.DAM_WINDSHLD or None)}' as DAM_WINDSHLD,
                        '{bool(row.STR_NOSE or None)}' as STR_NOSE,
                        '{bool(row.DAM_NOSE or None)}' as DAM_NOSE,
                        '{bool(row.STR_ENG1 or None)}' as STR_ENG1,
                        '{bool(row.DAM_ENG1 or None)}' as DAM_ENG1,
                        '{bool(row.ING_ENG1 or None)}' as ING_ENG1,
                        '{bool(row.STR_ENG2 or None)}' as STR_ENG2,
                        '{bool(row.DAM_ENG2 or None)}' as DAM_ENG2,
                        '{bool(row.ING_ENG2 or None)}' as ING_ENG2,
                        '{bool(row.STR_ENG3 or None)}' as STR_ENG3,
                        '{bool(row.DAM_ENG3 or None)}' as DAM_ENG3,
                        '{bool(row.ING_ENG3 or None)}' as ING_ENG3,
                        '{bool(row.STR_ENG4 or None)}' as STR_ENG4,
                        '{bool(row.DAM_ENG4 or None)}' as DAM_ENG4,
                        '{bool(row.ING_ENG4 or None)}' as ING_ENG4,
                        '{bool(row.STR_PROP or None)}' as STR_PROP,
                        '{bool(row.DAM_PROP or None)}' as DAM_PROP,
                        '{bool(row.STR_WING_ROT or None)}' as STR_WING_ROT,
                        '{bool(row.DAM_WING_ROT or None)}' as DAM_WING_ROT,
                        '{bool(row.STR_FUSE or None)}' as STR_FUSE,
                        '{bool(row.DAM_FUSE or None)}' as DAM_FUSE,
                        '{bool(row.STR_LG or None)}' as STR_LG,
                        '{bool(row.DAM_LG or None)}' as DAM_LG,
                        '{bool(row.STR_TAIL or None)}' as STR_TAIL,
                        '{bool(row.DAM_TAIL or None)}' as DAM_TAIL,
                        '{bool(row.STR_LGHTS or None)}' as STR_LGHTS,
                        '{bool(row.DAM_LGHTS or None)}' as DAM_LGHTS,
                        '{bool(row.STR_OTHER or None)}' as STR_OTHER,
                        '{bool(row.DAM_OTHER or None)}' as DAM_OTHER,
                        '{str(row.OTHER_SPECIFY or None).replace("'",'')}' as OTHER_SPECIFY,
                        '{str(row.EFFECT or None)}' as EFFECT,
                        '{str(row.EFFECT_OTHER or None).replace("'",'')}' as EFFECT_OTHER,
                        '{str(row.SPECIES_ID or None)}' as SPECIES_ID,
                        '{str(row.SPECIES or None).replace("'",'')}' as SPECIES,
                        '{str(row.OUT_OF_RANGE_SPECIES or None).replace("'",'')}' as OUT_OF_RANGE_SPECIES,
                        '{str(row.REMARKS or None).replace("'",'')}' as REMARKS,
                        '{str(row.REMAINS_COLLECTED or None).replace("'",'')}' as REMAINS_COLLECTED,
                        '{str(row.REMAINS_SENT or None)}' as REMAINS_SENT,
                        '{str(row.BIRD_BAND_NUMBER or None)}' as BIRD_BAND_NUMBER,
                        '{str(row.WARNED or None)}' as WARNED,
                        '{str(row.NUM_SEEN or None)}' as NUM_SEEN,
                        '{str(row.NUM_STRUCK or None)}' as NUM_STRUCK,
                        '{str(row.SIZE or None)}' as SIZE,
                        '{str(row.NR_INJURIES or None)}' as NR_INJURIES,
                        '{str(row.NR_FATALITIES or None)}' as NR_FATALITIES,
                        '{str(row.COMMENTS or None).replace("'",'')}' as COMMENTS,
                        '{str(row.REPORTER_NAME or None)}' as REPORTER_NAME,
                        '{str(row.REPORTER_TITLE or None)}' as REPORTER_TITLE,
                        '{str(row.SOURCE or None)}' as SOURCE,
                        '{str(row.PERSON or None)}' as PERSON,
                        '{str(row.LUPDATE or None)}' as LUPDATE,
                        '{bool(row.IMAGE or None)}' as IMAGE,
                        '{bool(row.TRANSFER or None)}' as TRANSFER
                        )
                        SELECT INDX_NR, INCIDENT_DATE, INCIDENT_MONTH, INCIDENT_YEAR, TIME, TIME_OF_DAY, AIRPORT_ID, 
                               AIRPORT, inc_coordinates, LATITUDE, LONGITUDE, RUNWAY, STATE, FAAREGION, LOCATION, 
                               ENROUTE_STATE, OPID, OPERATOR, REG, FLT, AIRCRAFT, AMA, AMO, EMA, EMO, AC_CLASS, AC_MASS, 
                               TYPE_ENG, NUM_ENGS, ENG_1_POS, ENG_2_POS, ENG_3_POS, ENG_4_POS, PHASE_OF_FLIGHT, HEIGHT, 
                               SPEED, DISTANCE, SKY, PRECIPITATION, AOS, COST_REPAIRS, COST_OTHER, 
                               COST_REPAIRS_INFL_ADJ, COST_OTHER_INFL_ADJ, INGESTED_OTHER, INDICATED_DAMAGE, 
                               DAMAGE_LEVEL, STR_RAD, DAM_RAD, STR_WINDSHLD, DAM_WINDSHLD, STR_NOSE, DAM_NOSE, STR_ENG1,
                               DAM_ENG1, ING_ENG1, STR_ENG2, DAM_ENG2, ING_ENG2, STR_ENG3, DAM_ENG3, ING_ENG3, STR_ENG4,
                               DAM_ENG4, ING_ENG4, STR_PROP, DAM_PROP, STR_WING_ROT, DAM_WING_ROT, STR_FUSE, DAM_FUSE, 
                               STR_LG, DAM_LG, STR_TAIL, DAM_TAIL, STR_LGHTS, DAM_LGHTS, STR_OTHER, DAM_OTHER, 
                               OTHER_SPECIFY, EFFECT, EFFECT_OTHER, SPECIES_ID, SPECIES, OUT_OF_RANGE_SPECIES, REMARKS, 
                               REMAINS_COLLECTED, REMAINS_SENT, BIRD_BAND_NUMBER, WARNED, NUM_SEEN, NUM_STRUCK, SIZE, 
                               NR_INJURIES, NR_FATALITIES, COMMENTS, REPORTER_NAME, REPORTER_TITLE, SOURCE, PERSON,
                               LUPDATE, IMAGE, TRANSFER
                        FROM cte
                        WHERE CAst(INDX_NR as int) NOT IN ({request_all_indexes}) ON CONFLICT DO NOTHING;"""
                    try:
                        cursor.execute(query)
                    except Exception as e:
                        self.logger.error(e)
                        unloaded_rows.append(row)
                        raise #pass
                connect.commit()
                if len(unloaded_rows) > 0:
                    self.logger.info(
                        f"{len(unloaded_rows)} rows were not loaded in table {self.schema}.{table_name}")
                    for record in unloaded_rows:
                        self.logger.warning(record)
                self.logger.info(f"Data loaded")
        self.result_files_list.clear()

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
            end_date = datetime.datetime.strptime(start_date, '%Y-%m-%d') + datetime.timedelta(weeks=2)
            end_date = datetime.datetime.strftime(end_date, '%Y-%m-%d')
        days_difference = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.datetime.strptime(start_date,
                                                                                                        '%Y-%m-%d')
        if not start_date < end_date:
            self.logger.warning(f"All data loaded for {end_date}")
        else:
            url = f"""https://wildlife.faa.gov/search"""  # роутер перебрасыват на страницу home, поэтому переход придется выполнять вручную
            response = req.get(url)

            # Зачищаем папку от zip файлов
            #[clean_directory(full_path=f"{os.getcwd()}/{file}")
            # for file in os.listdir(f"{os.getcwd()}") if file.endswith('.zip')]
            self.logger.info(f"Atempt to find data between {start_date} and {end_date}. Range {days_difference.days} days")
            if response.status_code == 200:
                options = webdriver.ChromeOptions()
                options.add_argument('headless')
                remote_webdriver = 'remote_chromedriver'
                with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
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
                    self.logger.info(f"Page {url} opened and filed, 10 seconds wait until data will be prepared")
                    time.sleep(10)
                    for i in range(1, 5):  # 5 попыток нажать на кнопку скачивания файла
                        try:
                            driver.find_element(By.CSS_SELECTOR,
                                                '#body > app-search > div.content > mat-card > mat-card-content > div > '
                                                'div.card.airport-information > div.card-body > '
                                                'div.card-footer.remove-margin.row > div.col-md-6.text-right.float-right > '
                                                'span:nth-child(2)').click()
                            self.logger.info("Download button pressed")
                            break
                        except Exception:
                            self.logger.warning(f"Attempt № {i} failed")
                            time.sleep(5)

                    time.sleep(days_difference.days / 2)  # Время для скачивания файла из расчета 0,5 секунды на загрузку 1 дня
                    for i in range(1, 8):
                        try:
                            current_file = [file for file in os.listdir(f"{os.getcwd()}/Downloads/") if file.endswith('.zip')][0]
                            break
                        except:
                            self.logger.warning(f"File .zip not found")
                            print(os.listdir(f"{os.getcwd()}"))
                            time.sleep(30)
                            pass
                    # Зачищаем целевую папку
                    #clean_directory(full_path=f"{os.getcwd()}/Downloads/{current_file}")
                    #shutil.move(src=f"{os.getcwd()}/{current_file}", dst=f"{os.getcwd()}/Downloads", )
                    self.logger.info(f"file {current_file} loaded to {os.getcwd()}/Downloads")
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
                if df.shape[0] > 0:
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
                else:
                    self.logger.info(f"No Data for choosen stations")
                print(file, self.downloaded_files_list)
                self.downloaded_files_list.remove(file)


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
            #driver = webdriver.Chrome(options=options)
            remote_webdriver = 'remote_chromedriver'
            with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=options) as driver:
                driver.get(URL)
                for i in range(15):
                    try:
                        time.sleep(15)
                        print(os.listdir(f"{os.getcwd()}"))
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
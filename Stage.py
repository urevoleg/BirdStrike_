# создание папок Downloads и Archives на уровень yaml

# 1 обработка URL
# 2 получение архива
# 3 обработка архива
import sys
import csv

import os
import zipfile
import logging
import datetime
import pandas as pd
from congig import Config

config = Config()
log = logging.getLogger(__name__)


def load_incidents(date: datetime.date,
                 pg_connect) -> None:
    """
    Получение данных об инцидентах с птицами и их запись в стейдж

    :param pg_connect:
    :param date:
    :return: None
    """
    arcive_list = os.listdir(f"{os.getcwd()}/Downloads")
    result_files_list = []
    for archive in arcive_list:
        with zipfile.ZipFile(f"{os.getcwd()}/Downloads/{archive}") as archive_file:
            files_list = archive_file.namelist()
            for file in files_list:
                archive_file.extract(member=file, path=f"{os.getcwd()}/Archives/{date}")
                log.info(msg=f"File {file} extracted in {os.getcwd()}/Archives/{date}")
                result_files_list.append(f"{os.getcwd()}/Archives/{date}/{file}")
    for file_name in result_files_list:
        df = pd.read_excel(io=file_name, sheet_name='data')
        result_df = df[['INDX_NR', 'INCIDENT_DATE ', 'LATITUDE', 'LONGITUDE', 'AIRPORT_ID', 'SPECIES']]
        result_df.rename(columns={'INCIDENT_DATE ': 'INCIDENT_DATE'}, inplace=True)
        log.info(f"Подготовлен Dataframe c {result_df.shape[0]} записями")
        schema = "STAGE"
        table_name = "aircraft_incidents"
        columns = 'INDX_NR, INCIDENT_DATE, LATITUDE, LONGITUDE, SPECIES'
        with pg_connect.connection() as connect:
            cursor = connect.cursor()
            unloaded_rows = []
            for row in result_df.itertuples():
                query = f"""
                            INSERT INTO {schema}.{table_name} ({columns}) VALUES
                            ('{row.INDX_NR}', '{row.INCIDENT_DATE}', '{row.LATITUDE}',
                                '{row.LONGITUDE}', {row.AIRPORT_ID}, '{row.SPECIES.replace("'", '')}')"""
                try:
                    cursor.execute(query)
                except Exception as e:
                    log.error(e)
                    unloaded_rows.append(row)
                    pass
            connect.commit()
            if len(unloaded_rows) > 0:
                log.info(f"{len(unloaded_rows)} записей не были загружены в таблицу {schema}.{table_name}")
                for record in unloaded_rows:
                    log.warning(record)

load_incidents(date=datetime.datetime.now().date(),
              pg_connect=config.pg_warehouse_db())



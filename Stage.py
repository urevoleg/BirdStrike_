# создание папок Downloads и Archives на уровень yaml

# 1 обработка URL
# 2 получение архива


# 3 обработка архива
import sys
import os
import zipfile
import logging
import datetime
import pandas as pd

log = logging.getLogger(__name__)


def open_archive(date: datetime.date) -> None:
    """Первая функция для получения данных об инцидентах с птицами и запись в стейдж"""
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
        result_df = df[['INDX_NR', 'INCIDENT_DATE ', 'LATITUDE', 'LONGITUDE', 'SPECIES']]
        # И запись в Postgres
        # Перенос файлов в корзину

today_files = open_archive(date=datetime.datetime.now().date())





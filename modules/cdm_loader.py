from datetime import datetime
from connections import PgConnect
import os
import shutil
import time
import zipfile
from selenium import webdriver
import requests as req
from datetime import datetime
from selenium.webdriver.common.by import By
import pandas as pd
from modules.instrumentals import clean_directory, table_extractor
from connections import PgConnect



class CdmControler:
    def __init__(self, date: datetime.date,
                 pg_connect: PgConnect,
                 schema: str,
                 logger):
        self.date = date
        self.pg_connect = pg_connect
        self.logger = logger
        self.schema = schema


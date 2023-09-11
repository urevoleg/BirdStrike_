from datetime import datetime
from .connections import PgConnect

class CdmControler:
    def __init__(self, date: datetime.date,
                 pg_connect: PgConnect,
                 schema: str,
                 logger):
        self.date = date
        self.pg_connect = pg_connect
        self.logger = logger
        self.schema = schema


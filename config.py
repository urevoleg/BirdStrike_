from modules.connections import PgConnect


class Config:

    def __init__(self) -> None:
        self.pg_warehouse_host = 'localhost' #os.environ.get("HOST")
        self.pg_warehouse_port = 6432 #5432
        self.pg_warehouse_dbname = "airflow"
        self.pg_warehouse_user = "airflow"
        self.pg_warehouse_password = "airflow"
        # port=os.environ.get("PORT"),
        # database=os.environ.get("DB"),
        # user=os.environ.get("DB_USERNAME"),
        # password=os.environ.get("DB_PASSWORD")

    def pg_warehouse_db(self):
        return PgConnect(host=self.pg_warehouse_host,
                         port=self.pg_warehouse_port,
                         db_name=self.pg_warehouse_dbname,
                         user=self.pg_warehouse_user,
                         pw=self.pg_warehouse_password)

from contextlib import contextmanager
from typing import Generator
from psycopg2 import connect


class PgConnect:
    def __init__(self, host: str, port: int, db_name: str, user: str, pw: str) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = pw

    def url(self) -> str:
        return f"""
                host={self.host}
                port={self.port}
                dbname={self.db_name}
                user={self.user}
                password={self.password}
                """

    @contextmanager
    def connection(self):
        conn = connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

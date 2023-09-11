FROM postgres:13

COPY Database/init_db.sql  /docker-entrypoint-initdb.d/init_db.sql
COPY Database/weather_2018_2021.sql  /docker-entrypoint-initdb.d/weather_2018_2021.sql
COPY Database/weather_2022.sql  /docker-entrypoint-initdb.d/weather_2022.sql
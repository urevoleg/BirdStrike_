FROM postgres:13

COPY Database/init_db.sql  /docker-entrypoint-initdb.d/init_db.sql
COPY Database/weather_observation.sql  /docker-entrypoint-initdb.d/weather_observation.sql

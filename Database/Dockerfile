FROM postgres:14

COPY Database/0_init_db.sql  /docker-entrypoint-initdb.d/0_init_db.sql
COPY Database/1_aircraft_incidents.sql  /docker-entrypoint-initdb.d/1_aircraft_incidents.sql
COPY Database/2_weather_observation.sql  /docker-entrypoint-initdb.d/2_weather_observation.sql


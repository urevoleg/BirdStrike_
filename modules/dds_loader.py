from datetime import datetime
from modules.connections import PgConnect


class DdsControler:
    def __init__(self, date: datetime.date,
                 pg_connect: PgConnect,
                 schema: str,
                 logger):
        self.date = date
        self.pg_connect = pg_connect
        self.logger = logger
        self.schema = schema

    def upload_aircraft_incidents(self, table_name: str) -> None:
        with self.pg_connect.connection() as connect:
            cursor = connect.cursor()
            query = f"""
            INSERT INTO {self.schema}.{table_name}
               (indx_nr, incident_date, inc_coordinates, airport_id, airport, species, INCIDENT_time)
               SELECT
                   indx_nr,                   
                   INCIDENT_DATE::date,
                   inc_coordinates::point,
                   AIRPORT_ID,
                   AIRPORT,
                   SPECIES,
                   REPLACE(TRIM(time), '%', '0')
               FROM STAGE.aircraft_incidents
               WHERE indx_nr not in (SELECT indx_nr FROM DDS.aircraft_incidents)
               ON CONFLICT DO NOTHING;
            """
            cursor.execute(query)
            connect.commit()
        self.logger.info(f"Aircraft incidents reference updated")

    def upload_weather_reference(self, table_name: str) -> None:
        with self.pg_connect.connection() as connect:
            connect.autocommit = False
            cursor = connect.cursor()
            query = f"""
            INSERT INTO {self.schema}.{table_name}
            (station, start_date, end_date, geo_data)
            SELECT
                station,
                cast(start_date as varchar)::date as start_date,
                cast(end_date as varchar)::date as end_date,
                geo_data
                FROM STAGE.observation_reference
            ON CONFLICT ON CONSTRAINT observation_reference_pkey DO UPDATE SET end_date = EXCLUDED.end_date;
            """
            cursor.execute(query)
            connect.commit()
        self.logger.info(f"Observation reference updated")

    def update_incident_station_link(self, table_name: str):
        with self.pg_connect.connection() as connect:
            connect.autocommit = False
            cursor = connect.cursor()
            query = f"""
            INSERT INTO {self.schema}.{table_name}
            (index_incedent, weather_station)
            WITH incident as (
                SELECT
                    indx_nr,
                    incident_date, 
                    inc_coordinates,
                    airport_id, 
                    airport, 
                    species 
                FROM DDS.aircraft_incidents incidents
                WHERE indx_nr not in (SELECT incident FROM DDS.incident_station_link)),
            stations as (
                SELECT
                    station, geo_data, start_date, end_date
                FROM DDS.observation_reference),
            calculation as (
                SELECT 
                       indx_nr, stations.station,
                       (inc_coordinates::point<->geo_data::point)*1.609 as kilometers,
                       ROW_NUMBER () 
                        OVER (
                            PARTITION BY indx_nr
                               ORDER BY (inc_coordinates::point<->geo_data::point)*1.609 ASC
                            ) as row_number
                FROM stations
                CROSS JOIN incident
                WHERE incident_date::date between start_date and end_date
                )
            SELECT
                indx_nr,
                station
            FROM calculation
            WHERE row_number = 1;
            """
            cursor.execute(query)
            connect.commit()
        self.logger.info(f"Table with links between stations and incidents updated")

    def upload_weather_observation(self, table_name: str, date) -> None:
        """Под вопросом"""
        with self.pg_connect.connection() as connect:
            connect.autocommit = False
            cursor = connect.cursor()
            query = f"""
                   INSERT INTO {self.schema}.{table_name}
                   (STATION, incident, weather_DATE, inc_date, WND, CIG, VIS, TMP, DEW, SLP)
        
                   WITH CTE as (SELECT
                    distinct station,
                    cast(DATE as timestamp) as date,
                    cast(concat(concat(cast(INCIDENT_DATE as varchar), ' '),cast(INCIDENT_time as varchar)) as timestamp) as inc_ddtm,
                    index_incedent,
                    weather.WND, 
                    weather.CIG, 
                    weather.VIS, 
                    weather.TMP, 
                    weather.DEW, 
                    weather.SLP 
                FROM STAGE.weather_observation weather
                INNER JOIN DDS.incident_station_link link ON weather.station = link.weather_station
                INNER JOIN DDS.aircraft_incidents inc ON inc.indx_nr=link.index_incedent
                WHERE index_incedent NOT IN (SELECT incident FROM {self.schema}.{table_name})
                AND INCIDENT_DATE <= cast(date as timestamp)
                ),
                calc as (
                SELECT *, date-inc_ddtm as diff
                FROM CTE),
                pre as (
                SELECT *, @EXTRACT(EPOCH FROM diff) as seconds
                FROM calc),
                result as (
                SELECT *, --station, inc_ddtm, WND, CIG, VIS, TMP, DEW, SLP,
                ROW_NUMBER () 
                                        OVER (
                                            PARTITION BY station,index_incedent
                                               ORDER BY seconds
                                            ) as row_number,
                                            seconds
                FROM pre)
                SELECT DISTINCT station, index_incedent, date, inc_ddtm, WND, CIG, VIS, TMP, DEW, SLP
                FROM result
                WHERE row_number=1
                ;
                                                    """
            cursor.execute(query)
            connect.commit()


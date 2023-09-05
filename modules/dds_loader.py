from datetime import datetime
from connections import PgConnect


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
            cursor.execute(f"""
            INSERT INTO {self.schema}.{table_name}
               (indx_nr, INCIDENT_DATE, LATITUDE, LONGITUDE, AIRPORT_ID, AIRPORT, SPECIES)
               SELECT
                   DISTINCT indx_nr, -- DISTINCT на всякий случай, но похорошему надо обсуждать                      
                   INCIDENT_DATE::date,
                   LATITUDE::DOUBLE PRECISION, 
                   LONGITUDE::DOUBLE PRECISION,
                   AIRPORT_ID,
                   AIRPORT,
                   SPECIES
               FROM STAGE.aircraft_incidents
               WHERE indx_nr not in (SELECT indx_nr FROM DDS.aircraft_incidents) -- дополнительная фильтрация, по хорошему данные уже были отфильтрованы при обработке pandas
               ;
            """)
            connect.commit()

    def upload_weather_observation(self, table_name: str, date: str) -> None:
        with self.pg_connect.connection() as connect:
            connect.autocommit = False
            cursor = connect.cursor()
            chosen_week = datetime.strptime(date, '%Y-%m-%d').date().strftime("%V")
            chosen_year = datetime.strptime(date, '%Y-%m-%d').year
            query = f"""
                   INSERT INTO {self.schema}.{table_name}
                   (STATION, DATE, LATITUDE, LONGITUDE, ELEVATION, NAME, TEMP,
                   TEMP_ATTRIBUTES, DEWP, DEWP_ATTRIBUTES, SLP, SLP_ATTRIBUTES, STP, STP_ATTRIBUTES,
                   VISIB, VISIB_ATTRIBUTES, WDSP, WDSP_ATTRIBUTES, MXSPD, GUST, MAX, MAX_ATTRIBUTES,
                   MIN, MIN_ATTRIBUTES, PRCP, PRCP_ATTRIBUTES, SNDP, FRSHTT)
        
                   SELECT
                               DISTINCT STATION, 
                               DATE::date,
                               LATITUDE::DOUBLE PRECISION,
                               LONGITUDE::DOUBLE PRECISION,
                               ELEVATION, NAME, TEMP,
                           TEMP_ATTRIBUTES, DEWP, DEWP_ATTRIBUTES, SLP, SLP_ATTRIBUTES, STP, STP_ATTRIBUTES,
                           VISIB, VISIB_ATTRIBUTES, WDSP, WDSP_ATTRIBUTES, MXSPD, GUST, MAX, MAX_ATTRIBUTES,
                           MIN, MIN_ATTRIBUTES, PRCP, PRCP_ATTRIBUTES, SNDP, FRSHTT
                           FROM STAGE.weather_observation
                           WHERE 1=1 
                                and EXTRACT('Year' FROM DATE::date) = {chosen_year} -- год совпадает с годом выборки
                                and STATION NOT IN ( 
                                                    SELECT STATION FROM DDS.weather_observation
                                                    WHERE 1=1 
                                                    and DATE_PART('week', DATE::date) = {chosen_week}
                                                    and EXTRACT('Year' FROM DATE::date) = {chosen_year}
                                                    ); -- id станции нет среди всех id за выбранный год и за выбранную неделю
                                                    """
            cursor.execute(query)
            connect.commit()

from datetime import datetime
from .connections import PgConnect


class DdsController:
    def __init__(self, date: datetime.date,
                 pg_connect: PgConnect,
                 schema: str,
                 logger):
        self.date = date
        self.pg_connect = pg_connect
        self.logger = logger
        self.schema = schema

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

    def upload_aircraft_incidents(self, table_name: str) -> None:
        with self.pg_connect.connection() as connect:
            cursor = connect.cursor()
            query = f"""
            INSERT INTO {self.schema}.{table_name}
               (
               INDX_NR, INCIDENT_DATE, INCIDENT_MONTH, INCIDENT_YEAR, TIME, TIME_OF_DAY, AIRPORT_ID, 
               AIRPORT, inc_coordinates, LATITUDE, LONGITUDE, RUNWAY, STATE, FAAREGION, LOCATION, 
               ENROUTE_STATE, OPID, OPERATOR, REG, FLT, AIRCRAFT, AMA, AMO, EMA, EMO, AC_CLASS, AC_MASS, 
               TYPE_ENG, NUM_ENGS, ENG_1_POS, ENG_2_POS, ENG_3_POS, ENG_4_POS, PHASE_OF_FLIGHT, HEIGHT, 
               SPEED, DISTANCE, SKY, PRECIPITATION, AOS, COST_REPAIRS, COST_OTHER, 
               COST_REPAIRS_INFL_ADJ, COST_OTHER_INFL_ADJ, INGESTED_OTHER, INDICATED_DAMAGE, 
               DAMAGE_LEVEL, STR_RAD, DAM_RAD, STR_WINDSHLD, DAM_WINDSHLD, STR_NOSE, DAM_NOSE, 
               STR_ENG1, DAM_ENG1, ING_ENG1, STR_ENG2, DAM_ENG2, ING_ENG2, STR_ENG3, DAM_ENG3, ING_ENG3, 
               STR_ENG4, DAM_ENG4, ING_ENG4, STR_PROP, DAM_PROP, STR_WING_ROT, DAM_WING_ROT, STR_FUSE, 
               DAM_FUSE, STR_LG, DAM_LG, STR_TAIL, DAM_TAIL, STR_LGHTS, DAM_LGHTS, STR_OTHER, DAM_OTHER, 
               OTHER_SPECIFY, EFFECT, EFFECT_OTHER, SPECIES_ID, SPECIES, OUT_OF_RANGE_SPECIES, 
               REMARKS, REMAINS_COLLECTED, REMAINS_SENT, BIRD_BAND_NUMBER, WARNED, NUM_SEEN, NUM_STRUCK, 
               SIZE, NR_INJURIES, NR_FATALITIES, COMMENTS, REPORTER_NAME, REPORTER_TITLE, SOURCE, PERSON, 
               LUPDATE, IMAGE, TRANSFER
               )

               SELECT
       cast (INDX_NR as int),
       CAST (INCIDENT_DATE as date),
       CAST (INCIDENT_MONTH as int),
       CAST (INCIDENT_YEAR as int),
       REPLACE(TRIM(time), '%', '0'),
       TIME_OF_DAY, AIRPORT_ID,
       AIRPORT,
       inc_coordinates,
       CAST(LATITUDE as float),
       CAST(LONGITUDE as float),
       RUNWAY, STATE, FAAREGION, LOCATION,
       ENROUTE_STATE, OPID, OPERATOR, REG, FLT, AIRCRAFT, AMA, AMO, EMA, EMO, AC_CLASS, AC_MASS,
       TYPE_ENG, NUM_ENGS, ENG_1_POS, ENG_2_POS, ENG_3_POS, ENG_4_POS, PHASE_OF_FLIGHT,
       CASE WHEN HEIGHT = 'None' or HEIGHT = '' THEN NULL ELSE CAST(HEIGHT as float) END as HEIGHT,
       CASE WHEN SPEED = 'None' or SPEED = '' THEN NULL ELSE CAST(SPEED as float) END as SPEED,
       CASE WHEN DISTANCE = 'None' or DISTANCE = '' THEN NULL ELSE CAST(DISTANCE as float) END as DISTANCE,
       SKY, PRECIPITATION,
       CASE WHEN AOS = 'None' or AOS = '' THEN NULL ELSE CAST(AOS as float) END as AOS,
       CASE WHEN COST_REPAIRS = 'None' or COST_REPAIRS = '' THEN NULL ELSE CAST(COST_REPAIRS as float) END as COST_REPAIRS,
       CASE WHEN COST_OTHER = 'None' or COST_OTHER = '' THEN NULL ELSE CAST(COST_OTHER as float) END as COST_OTHER,
       CASE WHEN COST_REPAIRS_INFL_ADJ = 'None' or COST_REPAIRS_INFL_ADJ = '' THEN NULL ELSE CAST(COST_REPAIRS_INFL_ADJ as float) END as COST_REPAIRS_INFL_ADJ,
       CASE WHEN COST_OTHER_INFL_ADJ = 'None' or COST_OTHER_INFL_ADJ = '' THEN NULL ELSE CAST(COST_OTHER_INFL_ADJ as float) END as COST_OTHER_INFL_ADJ,
       INGESTED_OTHER::bool,
       INDICATED_DAMAGE::bool,
       DAMAGE_LEVEL,
       STR_RAD::bool,
       DAM_RAD::bool,
       STR_WINDSHLD::bool,
       DAM_WINDSHLD::bool,
       STR_NOSE::bool,
       DAM_NOSE::bool,
       STR_ENG1::bool,
       DAM_ENG1::bool,
       ING_ENG1::bool,
       STR_ENG2::bool,
       DAM_ENG2::bool,
       ING_ENG2::bool,
       STR_ENG3::bool,
       DAM_ENG3::bool,
       ING_ENG3::bool,
       STR_ENG4::bool,
       DAM_ENG4::bool,
       ING_ENG4::bool,
       STR_PROP::bool,
       DAM_PROP::bool,
       STR_WING_ROT::bool,
       DAM_WING_ROT::bool,
       STR_FUSE::bool,
       DAM_FUSE::bool,
       STR_LG::bool,
       DAM_LG::bool,
       STR_TAIL::bool,
       DAM_TAIL::bool,
       STR_LGHTS::bool,
       DAM_LGHTS::bool,
       STR_OTHER::bool,
       DAM_OTHER::bool,
   OTHER_SPECIFY, EFFECT, EFFECT_OTHER, SPECIES_ID, SPECIES,
   CASE WHEN OUT_OF_RANGE_SPECIES = 'None' THEN NULL ELSE OUT_OF_RANGE_SPECIES::bool END as OUT_OF_RANGE_SPECIES,
   REMARKS,
   CASE WHEN REMAINS_COLLECTED = 'None' THEN NULL ELSE REMAINS_COLLECTED::bool END as REMAINS_COLLECTED,
   CASE WHEN REMAINS_SENT = 'None' THEN NULL ELSE REMAINS_SENT::bool END as REMAINS_SENT,
   BIRD_BAND_NUMBER, WARNED, NUM_SEEN, NUM_STRUCK,
   SIZE, NR_INJURIES, NR_FATALITIES, COMMENTS, REPORTER_NAME, REPORTER_TITLE, SOURCE, PERSON,
   LUPDATE::date,
   IMAGE::bool,
   TRANSFER::bool
   FROM STAGE.aircraft_incidents
   WHERE cast(indx_nr as int) not in (SELECT indx_nr FROM DDS.aircraft_incidents)
               ON CONFLICT DO NOTHING;
            """
            cursor.execute(query)
            connect.commit()
        self.logger.info(f"Aircraft incidents reference updated")

    def update_incident_station_link(self, table_name: str):
        with self.pg_connect.connection() as connect:
            connect.autocommit = False
            cursor = connect.cursor()
            self.logger.info('Updating links started')
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
                WHERE indx_nr not in (SELECT index_incedent FROM DDS.incident_station_link)),
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

    def upload_weather_observation(self, table_name: str) -> None:
        """Под вопросом"""
        with self.pg_connect.connection() as connect:
            connect.autocommit = False
            cursor = connect.cursor()
            query = f"""
                   SET datestyle = dmy;
                   INSERT INTO {self.schema}.{table_name}
                   (STATION, incident, weather_DATE, inc_date, WND, CIG, VIS, TMP, DEW, SLP)

                   WITH CTE as (SELECT
                    distinct station,
                    cast(DATE as timestamp) as date,
                    cast(concat(concat(cast(INCIDENT_DATE as varchar), ' '),cast(time as varchar)) as timestamp) as inc_ddtm,
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
                WHERE index_incedent NOT IN (SELECT cast(incident as int) FROM {self.schema}.{table_name})
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
            print(query)
            cursor.execute(query)
            connect.commit()

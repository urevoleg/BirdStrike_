CREATE SCHEMA IF NOT EXISTS STAGE;
CREATE SCHEMA IF NOT EXISTS DDS;
CREATE SCHEMA IF NOT EXISTS CDM;

DO $$
BEGIN
CREATE ROLE airflow superuser;
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

ALTER ROLE airflow login;

CREATE TABLE IF NOT EXISTS STAGE.aircraft_incidents -- таблица для сырых данных об авиационных инцидентах
    (INDX_NR text,
    INCIDENT_DATE text,
    INCIDENT_MONTH text,
    INCIDENT_YEAR text,
    TIME text,
    TIME_OF_DAY text,
    AIRPORT_ID text,
    AIRPORT text,
    inc_coordinates point,
    LATITUDE text,
    LONGITUDE text,
    RUNWAY text,
    STATE text,
    FAAREGION text,
    LOCATION text,
    ENROUTE_STATE text,
    OPID text,
    OPERATOR text,
    REG text,
    FLT text,
    AIRCRAFT text,
    AMA text,
    AMO text,
    EMA text,
    EMO text,
    AC_CLASS text,
    AC_MASS text,
    TYPE_ENG text,
    NUM_ENGS text,
    ENG_1_POS text,
    ENG_2_POS text,
    ENG_3_POS text,
    ENG_4_POS text,
    PHASE_OF_FLIGHT text,
    HEIGHT text,
    SPEED text,
    DISTANCE text,
    SKY text,
    PRECIPITATION text,
    AOS text,
    COST_REPAIRS text,
    COST_OTHER text,
    COST_REPAIRS_INFL_ADJ text,
    COST_OTHER_INFL_ADJ text,
    INGESTED_OTHER text,
    INDICATED_DAMAGE text,
    DAMAGE_LEVEL text,
    STR_RAD text,
    DAM_RAD text,
    STR_WINDSHLD text,
    DAM_WINDSHLD text,
    STR_NOSE text,
    DAM_NOSE text,
    STR_ENG1 text,
    DAM_ENG1 text,
    ING_ENG1 text,
    STR_ENG2 text,
    DAM_ENG2 text,
    ING_ENG2 text,
    STR_ENG3 text,
    DAM_ENG3 text,
    ING_ENG3 text,
    STR_ENG4 text,
    DAM_ENG4 text,
    ING_ENG4 text,
    STR_PROP text,
    DAM_PROP text,
    STR_WING_ROT text,
    DAM_WING_ROT text,
    STR_FUSE text,
    DAM_FUSE text,
    STR_LG text,
    DAM_LG text,
    STR_TAIL text,
    DAM_TAIL text,
    STR_LGHTS text,
    DAM_LGHTS text,
    STR_OTHER text,
    DAM_OTHER text,
    OTHER_SPECIFY text,
    EFFECT text,
    EFFECT_OTHER text,
    SPECIES_ID text,
    SPECIES text,
    OUT_OF_RANGE_SPECIES text,
    REMARKS text,
    REMAINS_COLLECTED text,
    REMAINS_SENT text,
    BIRD_BAND_NUMBER text,
    WARNED text,
    NUM_SEEN text,
    NUM_STRUCK text,
    SIZE text,
    NR_INJURIES text,
    NR_FATALITIES text,
    COMMENTS text,
    REPORTER_NAME text,
    REPORTER_TITLE text,
    SOURCE text,
    PERSON text,
    LUPDATE text,
    IMAGE text,
    TRANSFER text
    );
















CREATE TABLE IF NOT EXISTS STAGE.observation_reference -- таблица для сырых данных о метеостанциях на территории США
    (station varchar(40),
    start_date int,
    end_date int,
    GEO_DATA point
    );

CREATE TABLE IF NOT EXISTS STAGE.weather_observation -- таблица для сырых данных о состоянии погоды
    (STATION varchar(40),
    incident varchar(40),
    DATE varchar(40),
    WND varchar(40),
    CIG varchar(40),
    VIS varchar(40),
    TMP varchar(40),
    DEW varchar(40),
    SLP varchar(40)
    );


CREATE TABLE IF NOT EXISTS DDS.aircraft_incidents -- таблица с обработанными данными об авиационных инцидентах
    (INDX_NR int PRIMARY KEY,
    INCIDENT_DATE date,
    INCIDENT_MONTH int,
    INCIDENT_YEAR int,
    TIME varchar(40),
    TIME_OF_DAY varchar(5),
    AIRPORT_ID varchar(10),
    AIRPORT varchar(60),
    inc_coordinates point,
    LATITUDE double precision,
    LONGITUDE double precision,
    RUNWAY varchar(50),--ALTER TABLE DDS.aircraft_incidents ALTER COLUMN RUNWAY TYPE varchar(50);
    STATE varchar(5),
    FAAREGION varchar(10),
    LOCATION text,
    ENROUTE_STATE varchar(40),
    OPID varchar(8),
    OPERATOR varchar(50),
    REG varchar(25), --ALTER TABLE DDS.aircraft_incidents ALTER COLUMN REG TYPE varchar(25);
    FLT varchar(20), --ALTER TABLE DDS.aircraft_incidents ALTER COLUMN FLT TYPE varchar(20);
    AIRCRAFT varchar(20),
    AMA varchar(10),
    AMO varchar(10),
    EMA varchar(10),
    EMO varchar(10),
    AC_CLASS varchar(5),
    AC_MASS varchar(5),
    TYPE_ENG varchar(5),
    NUM_ENGS varchar(5),
    ENG_1_POS varchar(5),
    ENG_2_POS varchar(5),
    ENG_3_POS varchar(5),
    ENG_4_POS varchar(5),
    PHASE_OF_FLIGHT varchar(20),
    HEIGHT float,
    SPEED float,
    DISTANCE float,
    SKY varchar(40),
    PRECIPITATION varchar(20),
    AOS int,
    COST_REPAIRS int,
    COST_OTHER int,
    COST_REPAIRS_INFL_ADJ int,
    COST_OTHER_INFL_ADJ int,
    INGESTED_OTHER boolean,
    INDICATED_DAMAGE boolean,
    DAMAGE_LEVEL varchar(10),
    STR_RAD boolean,
    DAM_RAD boolean,
    STR_WINDSHLD boolean,
    DAM_WINDSHLD boolean,
    STR_NOSE boolean,
    DAM_NOSE boolean,
    STR_ENG1 boolean,
    DAM_ENG1 boolean,
    ING_ENG1 boolean,
    STR_ENG2 boolean,
    DAM_ENG2 boolean,
    ING_ENG2 boolean,
    STR_ENG3 boolean,
    DAM_ENG3 boolean,
    ING_ENG3 boolean,
    STR_ENG4 boolean,
    DAM_ENG4 boolean,
    ING_ENG4 boolean,
    STR_PROP boolean,
    DAM_PROP boolean,
    STR_WING_ROT boolean,
    DAM_WING_ROT boolean,
    STR_FUSE boolean,
    DAM_FUSE boolean,
    STR_LG boolean,
    DAM_LG boolean,
    STR_TAIL boolean,
    DAM_TAIL boolean,
    STR_LGHTS boolean,
    DAM_LGHTS boolean,
    STR_OTHER boolean,
    DAM_OTHER boolean,
    OTHER_SPECIFY text,
    EFFECT text,
    EFFECT_OTHER text,
    SPECIES_ID varchar(15),
    SPECIES varchar(200),
    OUT_OF_RANGE_SPECIES boolean,
    REMARKS text,
    REMAINS_COLLECTED boolean,
    REMAINS_SENT boolean,
    BIRD_BAND_NUMBER varchar(40),
    WARNED varchar(20),
    NUM_SEEN varchar(20),
    NUM_STRUCK varchar(20),
    SIZE varchar(20),
    NR_INJURIES varchar(40),
    NR_FATALITIES varchar(40),
    COMMENTS text,
    REPORTER_NAME varchar(50),
    REPORTER_TITLE varchar(50),
    SOURCE varchar(40),
    PERSON varchar(50),
    LUPDATE date,
    IMAGE boolean,
    TRANSFER boolean
    );

CREATE TABLE IF NOT EXISTS DDS.observation_reference -- таблица с обработанными данными о метеостанциях на территории США
    (station varchar(40) PRIMARY KEY,
    start_date date,
    end_date date,
    GEO_DATA point
    );

CREATE TABLE IF NOT EXISTS DDS.incident_station_link --таблица с инцидентами и ближайшей метеостанцией
    (index_incedent int,
    weather_station varchar(40),
    FOREIGN KEY(index_incedent)
    REFERENCES DDS.aircraft_incidents(indx_nr),
    FOREIGN KEY(weather_station)
    REFERENCES DDS.observation_reference(station)
    );


CREATE TABLE IF NOT EXISTS DDS.weather_observation -- таблица с обработанными данными о погоде
    (STATION varchar(40),
    incident varchar(40),
    weather_DATE timestamp,
    inc_date timestamp,
    WND varchar(40),
    CIG varchar(40),
    VIS varchar(40),
    TMP varchar(40),
    DEW varchar(40),
    SLP varchar(40)
    );


CREATE TABLE IF NOT EXISTS CDM.top_ten_airports -- витрина с данными о ТОП-10 аэропортах по числу инцидентов на дату построения
(id serial,
    airport_id varchar(40),
    airport_name varchar(200),
    Year varchar(20),
    month varchar(20),
    origin_domestic varchar(20),
    origin_international varchar(20),
    origin_total varchar(20),
    destination_domestic varchar(20),
    destination_international varchar(20),
    destination_total varchar(20),
    report_dt date
    );


CREATE TABLE IF NOT EXISTS DDS.airport_bts_name -- справочник с именами ажророртов согласно номенклатуре BTS
    (id varchar(50) primary key,
    bts_name varchar null);

INSERT INTO DDS.airport_bts_name -- заполнение некоторых аэропортов именами согласно номенклатуре BTS
(id, bts_name)
values
('KDEN', '- DENVER, CO: Denver international'),
('KDFW', '- DALLAS/FORT WORTH'),
('KORD', '- CHICAGO, IL: CHICAGO OHARE'),
('KATL', '- Atlanta, GA: Hartsfield-Jackson Atlanta international'),
('KDTW', '- DETROIT, ML: DETROIT METRO WAYNE COUNTY'),
('KMEM', '- MEMPHIS, TN: MEMPHIS international'),
('KJFK', '- New York, NY: John F. Kennedy International'),
('KMCO', '- Orlando, FL: Orlando International'),
('KCLT', '- CHARLOTTE, NC: Charlotte/Douglas International'),
('KSLC', '- Salt Lake City, UT: Salt Lake City International');


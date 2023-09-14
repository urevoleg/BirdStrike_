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
    (indx_nr varchar(40),
    INCIDENT_DATE varchar(40),
    inc_coordinates point,
    AIRPORT_ID varchar(40),
    AIRPORT varchar(100),
    SPECIES varchar(200),
    time varchar(50)
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
    (indx_nr varchar(40) PRIMARY KEY,
    INCIDENT_DATE date,
    inc_coordinates point,
    AIRPORT_ID varchar(40),
    AIRPORT varchar(100),
    SPECIES varchar(200),
    INCIDENT_time varchar(50)
    );

CREATE TABLE IF NOT EXISTS DDS.observation_reference -- таблица с обработанными данными о метеостанциях на территории США
    (station varchar(40) PRIMARY KEY,
    start_date date,
    end_date date,
    GEO_DATA point
    );


CREATE TABLE IF NOT EXISTS DDS.incident_station_link --таблица с инцидентами и ближайшей метеостанцией
    (index_incedent varchar(40),
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


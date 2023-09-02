CREATE SCHEMA IF NOT EXISTS STAGE;

CREATE TABLE IF NOT EXISTS STAGE.aircraft_incidents
    (indx_nr varchar(40),
    INCIDENT_DATE varchar(40),
    LATITUDE varchar(40),
    LONGITUDE varchar(40),
    AIRPORT_ID varchar(40),
    SPECIES varchar(200)
    );
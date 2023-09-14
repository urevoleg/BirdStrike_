"""
WITH CTE as (SELECT
                    distinct station,
                    cast(date as timestamp) as date,
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
                WHERE index_incedent NOT IN (SELECT incident FROM DDS.weather_observation)
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
                SELECT DISTINCT station, index_incedent, inc_ddtm, WND, CIG, VIS, TMP, DEW, SLP
                FROM result
                WHERE row_number=1

"""
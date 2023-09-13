import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd


def years_extractor(start_date: str,
                    end_date: str):
    datetime_start_date = datetime.strptime(start_date, '%Y-%m-%d')
    datetime_end_date = datetime.strptime(end_date, '%Y-%m-%d')
    delta = datetime_end_date - datetime_start_date

    years = list(set([(datetime_start_date + timedelta(days=i)).year for i in range(delta.days + 1)]))
    return sorted(years)


def get_stations(db_connection, schema, table_name, year) -> list:
    with db_connection.connection() as connect:
        cursor = connect.cursor()
        query = f"""SELECT DISTINCT station FROM {schema}.{table_name} 
                    WHERE to_date('{year}', 'YYYY') between start_date and end_date;"""
        cursor.execute(query)
        stations = cursor.fetchall()
        return [x[0] for x in stations]

def get_incidents_stations(db_connection, schema, table_name, year):
    with db_connection.connection() as connect:
        cursor = connect.cursor() #
        query = f"""SELECT DISTINCT weather_station FROM {schema}.{table_name}
                       JOIN  DDS.aircraft_incidents ON incident_station_link.index_incedent= aircraft_incidents.indx_nr
                       WHERE EXTRACT('YEAR' FROM incident_date) = '{year}';"""
        cursor.execute(query)
        stations = cursor.fetchall()
        return [x[0] for x in stations]

def get_field_stations(db_connection, schema, table_name, year):
    with db_connection.connection() as connect:
        cursor = connect.cursor()
        query = f"""SELECT DISTINCT station FROM {schema}.{table_name} 
                    WHERE extract('Year' FROM date)={int(year)}
                            ;"""
        cursor.execute(query)
        stations = cursor.fetchall()
        return [x[0] for x in stations]


def clean_directory(full_path):
    try:
        os.remove(full_path)
    except:
        pass


def table_extractor(html):
    soup = BeautifulSoup(html, 'lxml')
    table1 = soup.find('table', cellspacing="1")
    headers = []
    for i in table1.find_all('th'):
        title = i.text
        headers.append(title)
    table = pd.DataFrame(columns=headers)
    for j in table1.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(table)
        table.loc[length] = row
    return table

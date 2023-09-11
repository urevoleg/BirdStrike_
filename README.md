# BirdStrike
<<<<<<< HEAD
PET project. Database about aircraft birdstrike incidents 


Структура проекта
Инструкция для создания контейнера с базой данных расположена в файле docker-compose.yaml
Код приложения помещен в файл main.py
В модуле config.py содержатся данные для создания клиентов для подключений к базам данных.
    Пока что это только подключение к POSTGRESQL  
В пакете modules содержатся модули для работы с разными слоями данных
    Модуль connections.py с кодом создания клиентов для подключений 
    Модуль stg_loader содержит класс StgControler для создания объектов обработки данных при заполении stage.
    Модуль dds_loader содержит класс DdsControler для инкрементальной загрузки данных в DDS слой.
    Модуль cdm_loader содержит класс CdmControler, создан на перспективу
    Модуль instrumental содержит функции, которые пока что прямо не отнесены 


На текущем этапе разработки проект запускается локально с использованием базы данных в docker  
Файл docker-compose.yaml с помощью команды docker-compose up -d создает докер-контейнер с СУБД POSTGRESQL  
Проверить работостособность базы данных можно внутри контейнера с помощью команды:
docker exec -it birdstrike-postgres-1 psql -U docker_app -d docker_app_db
Далее можно вводить sql команды через командну строку




Этапы выполнения кода
Заполнение STAGE слоя
    Пользователь определяет диапазон даныых для заполнения таблиц в стейдж слое
    Выполняется загрузка данных о погоде
        Это самая долгая часть работы. Ее нужно будет выделить в отдельный ДАГ или распарралелить выполнение.
        На сайте National Centers for Environmental Information подробные сведения о погоде хранятся по годам и по станциям.
        Обобщенные данные по станциям очень ограничены. Поэтому при создании базы данных, в нее сразу же заливается пулл последних актуальных данных по станциям (индивидуальные номера)
        В будущем стоит предусмотреть механизм получения этих данных с сайта автоматически до первого заполенния скрипта, а также их обновления перед каждым запуском.
        Поскольку подробные сведения о погоде хранятся по годам и по станциям. сначала определяются годы, на которые необходимо искать сведения. Потом на указанный год, определяется о каких станций нет данных в dds слое на указанный год.
        Далее через selenium webdriver выполняется скачивание файла с наблюдениями за год для каждой станции. В будущем этот этап следует распаралелить на несколько одновременных обработок.
        Код отбирает из excel файла несколько столбцов и сохраняет из в таблицу STG.weather_observation
    Выполняется загрузка данных о авиационных инцидентах с участием животных
        На заданные пользователем даты отбирается выборка из базы данных Federal Aviation Administration через selenium webdriver. Чем больше выборка, тем больше времени нужно чтобы сайт собрал excel файл и тем больше времени нужно selenium webdriver чтобы скачать файл.
        Поэтому до перехода к следующему этапу выполняется time.sleep(60), однако на выборке больше 1 года и при плохом соединении, этого может оказаться недостаточно
        Полученный файл рахархивируется и сохраняется в dataframe, после чего заливается в базу данных
        Важно! Перед заливкой выполняется преобразование типов данных в 2-х колонках
            result_df = result_df.astype({'LATITUDE': str, 'LONGITUDE': str})
        Это вынужденная мера, в связи с тем, что в 2022 году неким пользователем введены координаты в другом формате "49°09'05?N" 16°41'40?E, в перспективе нужно будет предусмотреть обработчик таких записей на этапе работы с dataframe в pandas
        **Важно станции открываются и закрываются - значит справочник нужно будет выполнить в 2SCD и делать выборку на актуальную дату.**
            Например в 2019 году еще нет станций ['99999900496', '99999900497', '99999926567', 'A0003093795', '78520300398', '99999926566']
    Все полученные данные инкрементально заливаются в DDS слой. Заливка не оптимальная, но позволяет фильтровать дубли. 
        Кроме того заливка в DDS слой нужна для данных о погоде, чтобы сократить объем данных заливаемых в Stage слой
    Загрузка данных о 10 аэропортах с наибольшим числом инцидетов с животными
        Из таблицы DDS.aircraft_incidents выбираются 11 аэропортоа с наибольшим числом инцидетов, Через inner join выполняется соединение выборки с табилцей DDS.airport_bts_name.  
        DDS.airport_bts_name пока создается вручную. Это справочник с названиями аэропортов на сайте Bureau of Transport Statistics
        В pезультате Inner join происходит фильтрация аэропортов, не указанных в справочнике. Это плохо, но сейчас это удаляет из выборки 11 аэропорт с названием "UNKNOWN"
        Созданный список tuples_airports из id, названия аэропорта по система Federal Aviation Administration и названия аэроторта по системе Bureau of Transport Statistics передается методу "top_airports_traffic"
        С помощью Selenium webdriver открывается страница Bureau of Transport Statistics, выбирается вкладка 'Link_Flights'. Каждый аэропорт в tuples_airports обрабатывается, вызывается таблица со вседениями об отбывающих рейсах и о прибывающих рейсах.
        Полученные записи записываются в Stage.top_ten_airports с указанием id, названия, статистики и даты построения выборки.


Структура Базы данных
    Схема Stage
        таблицы
    Схема DDS
        таблицы
    Схема CDM
        пустая



docker exec -it birdstrike-airflow-scheduler-1 bash
docker exec -it birdstrike-airflow-worker-1 bash
docker cp requirements.txt  birdstrike-airflow-worker-1:/.

pip install selenium

pip install psycopg2-binary==2.8







https://wildlife.faa.gov/search -- ссылка-источник для получения данных об инцидентах с животными
Date Range From: 2018-01-01
За 5 лет - это слишком много
Лучше брать небольшими кусочками по 1 году или менее
Date Range To: 2022-01-01










Column Name Explanation of Column Name and Codes
INDEX NR Individual record number
OPID Airline operator code
OPERATOR
A three letter International Civil Aviation Organization code for aircraft operators. (BUS = business, PVT = private aircraft other than business, GOV =
government aircraft, MIL - military aircraft.)
ATYPE Aircraft
AMA International Civil Aviation Organization code for Aircraft Make
AMO International Civil Aviation Organization code for Aircraft Model
EMA Engine Make Code (see Engine Codes tab below)
EMO Engine Model Code (see Engine Codes tab below)
AC_CLASS Type of aircraft (see Aircraft Type tab below)
AC_MASS 1 = 2,250 kg or less: 2 = ,2251-5700 kg: 3 = 5,701-27,000 kg: 4 = 27,001-272,000 kg: 5 = above 272,000 kg
NUM_ENGS Number of engines
TYPE_ENG Type of power A = reciprocating engine (piston): B = Turbojet: C = Turboprop: D = Turbofan: E = None (glider): F = Turboshaft (helicopter): Y = Other
ENG_1_POS Where engine # 1 is mounted on aircraft (see Engine Position tab below)
ENG_2_POS Where engine # 2 is mounted on aircraft (see Engine Position tab below)
ENG_3_POS Where engine # 3 is mounted on aircraft (see Engine Position tab below)
ENG_4_POS Where engine # 4 is mounted on aircraft (see Engine Position tab below)
REG Aircraft registration
FLT Flight number
REMAINS_COLLECTED Indicates if bird or wildlife remains were found and collected
REMAINS_SENT Indicates if remains were sent to the Smithsonian Institution for identifcation
INCIDENT_DATE Date strike occurred
INCIDENT_MONTH Month strike occurred
INCIDENT_YEAR Year strike occurred
TIME_OF_DAY Light conditions
TIME Hour and minute in local time
AIRPORT_ID International Civil Aviation Organization airport identifier for location of strike whether it was on or off airport
AIRPORT Name of airport
STATE State
FAAREGION FAA Region where airport is located
ENROUTE If strike did not occur on approach, climb, landing roll, taxi or take-off, aircraft was enroute. This shows location.
RUNWAY Runway
LOCATION
Various information about aircraft location if enroute or airport where strike evidence was found. Some locations show the two airports for the flight
departure and arrival if pilot was unaware of the strike.
HEIGHT Feet Above Ground Level
SPEED Knots (indicated air speed)
DISTANCE Nautical miles from airport
PHASE_OF_FLT Phase of flight during which strike occurred
DAMAGE Level of damage selected by the Database Manager. See below for ICAO definitions taken from ICAO IBIS Manual Fourth Edition-2001
Blank Unknown
N = None No damage was reported.
M = Minor When the aircraft can be rendered airworthy by simple repairs or replacements and an extensive inspection is not necessary.
M? = Undetermined level The aircraft was damaged, but details as to the extent of the damage are lacking.
S = Substantial
When the aircraft incurs damage or structural failure which adversely affects the structure strength, performance or flight characteristics of the aircraft
and which would normally require major repair or replacement of the affected component. Bent fairings or cowlings; small dents or puncture holes in
the skin; damage to wing tips, antennae, tires or brakes; and engine blade damage not requiring blade replacement are specifically excluded.
D = Destroyed When the damage sustained makes it inadvisable to restore the aircraft to an airworthy condition.
STR_RAD Struck radome
DAM_RAD Damaged radome
STR_WINDSHLD Struck windshield
DAM_WINDSHLD Damaged windshield
STR_NOSE Struck nose
DAM_NOSE Damaged nose
STR_ENG1 Struck Engine 1
DAM_ENG1 Damaged Engine 1
ING_ENG1 Ingested Engine 1
STR_ENG2 Struck Engine 2
DAM_ENG2 Damaged Engine 2
ING_ENG2 Ingested Engine 2
STR_ENG3 Struck Engine 3
DAM_ENG3 Damaged Engine 3
ING_ENG3 Ingested Engine 3
STR_ENG4 Struck Engine 4
DAM_ENG4 Damaged Engine 4
ING_ENG4 Ingested Engine 4
INGESTED_OTHER Wildlife ingested in a location other than an engine, effective 3/29/2021 (ALL ingestions for strikes submitted prior to 3/29/2021 are shown here)
STR_PROP Struck Propeller
DAM_PROP Damaged Propeller
STR_WING_ROT Struck Wing or Rotor
DAM_WING_ROT Damaged Wing or Rotor
STR_FUSE Struck Fuselage
DAM_FUSE Damaged Fuselage
STR_LG Struck Landing Gear
DAM_LG Damaged Landing Gear
STR_TAIL Struck Tail
DAM_TAIL Damaged Tail
STR_LGHTS Struck Lights
DAM_LGHTS Damaged Lights
STR_OTHER Struck Other than parts shown above
DAM_OTHER Damaged Other than parts shown above
OTHER_SPECIFY What part was struck other than those listed above
EFFECT Effect on flight
EFFECT_OTHER Effect on flight other than those listed on the form
SKY Type of cloud cover, if any
PRECIP Precipitation
BIRD_BAND_NUMBER Bird Band Number associated with the wildlife struck
SPECIES_ID International Civil Aviation Organization code for type of bird or other wildlife
SPECIES Common name for bird or other wildlife
BIRDS_SEEN Number of birds/wildlife seen by pilot
BIRDS_STRUCK Number of birds/wildlife struck
SIZE
Size of bird as reported by pilot is a relative scale. Entry should reflect the perceived size as opposed to a scientifically determined value. If more than
one species was struck, larger bird is entered.
WARNED Pilot warned of birds/wildlife
COMMENTS As entered by database manager. Can include name of aircraft owner, types of reports received, updates, etc.
REMARKS Most of remarks are from the form but some are data entry notes and are usually in parentheses.
AOS Time aircraft was out of service in hours. If unknown, it is blank.
COST_REPAIRS Estimated cost of repairs of replacement in dollars (USD)
COST_OTHER
Estimated other costs, other than those in previous field in dollars (USD). May include loss of revenue, hotel expenses due to flight cancellation, costs
of fuel dumped, etc.
COST_REPAIRS_INFL_ADJ Costs adjusted to the most recent year based on Consumer Price Index, U.S. Department of Labor. Inflation-adjusted costs are updated annually.
COST_OTHER_INFL_ADJ Costs adjusted to the most recent year based on Consumer Price Index, U.S. Department of Labor. Inflation-adjusted costs are updated annually.
REPORTED_NAME Name(s) of person(s) filing report
REPORTED_TITLE Title(s) of person(s) filing report
REPORTED_DATE Date report was written
SOURCE Type of report. Note: for multiple types of reports this will be indicated as Multiple. See "Comments" field for details
PERSON Only one selection allowed. For multiple reports, see field "Reported Title"
NR_INJURIES Number of people injured
NR_FATALITIES Number of human fatalities
LUPDATE Last time record was updated
TRANSFER Unused field at this time
INDICATED_DAMAGE Indicates whether or not aircraft was damaged
=======
PET project. Database about birdstrike aircraft incidents 
>>>>>>> origin/main








#######


Данные по станциям
https://www.ncei.noaa.gov/access/search/data-search/global-summary-of-the-day?startDate=2018-01-01T00:00:00&endDate=2018-01-31T23:59:59&pageNum=1&stations=99999963826


TEMP - Mean temperature (.1 Fahrenheit)
DEWP - Mean dew point (.1 Fahrenheit)
SLP - Mean sea level pressure (.1 mb)
STP - Mean station pressure (.1 mb)
VISIB - Mean visibility (.1 miles)
WDSP – Mean wind speed (.1 knots)
MXSPD - Maximum sustained wind speed (.1 knots)
GUST - Maximum wind gust (.1 knots)
MAX - Maximum temperature (.1 Fahrenheit)
MIN - Minimum temperature (.1 Fahrenheit)
PRCP - Precipitation amount (.01 inches)
SNDP - Snow depth (.1 inches)
FRSHTT – Indicator for occurrence of:
 Fog
 Rain or Drizzle
 Snow or Ice Pellets
 Hail
 Thunder
 Tornado/Funnel Cloud


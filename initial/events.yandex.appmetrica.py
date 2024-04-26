# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
import time
import numpy as np

# импорт настроек
import configparser
config = configparser.ConfigParser()
config.read("../settings.ini")

# подключение к БД
if config["DB"]["TYPE"] == "MYSQL":
	engine = create_engine('mysql+mysqlclient://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "POSTGRESQL":
    engine = create_engine('postgresql+psycopg2://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + '/' + config["DB"]["DB"] + '?client_encoding=utf8')
elif config["DB"]["TYPE"] == "MARIADB":
    engine = create_engine('mysql+mysqldb://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "ORACLE":
    engine = create_engine('oracle+pyodbc://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + '/' + config["DB"]["DB"])
elif config["DB"]["TYPE"] == "SQLITE":
    engine = create_engine('sqlite:///' + config["DB"]["DB"])

# создание подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection = engine.raw_connection()
    if config["DB"]["TYPE"] in ["MYSQL", "MARIADB"]:
        connection.execute(text('SET NAMES utf8mb4'))
        connection.execute(text('SET CHARACTER SET utf8mb4'))
        connection.execute(text('SET character_set_connection=utf8mb4'))

# создаем таблицу для данных при наличии каких-либо данных
table_not_created = True
# выгружаем данные за 5 лет по месяцам
for period in range(int(config["YANDEX_APPMETRICA"]["PERIODS"]), 0, -1):
# Создание запроса на выгрузку данных (помесячно)
    fields = "event_datetime,event_json,event_name,event_receive_datetime,event_receive_timestamp,event_timestamp,session_id,installation_id,appmetrica_device_id,city,connection_type,country_iso_code,device_ipv6,device_locale,device_manufacturer,device_model,device_type,google_aid,ios_ifa,ios_ifv,mcc,mnc,operator_name,original_device_model,os_name,os_version,profile_id,windows_aid,app_build_number,app_package_name,app_version_name,application_id"
    date_since = (date.today() - timedelta(days=int(config["YANDEX_APPMETRICA"]["DELTA"])*period)).strftime('%Y-%m-%d')
    date_until = (date.today() - timedelta(days=int(config["YANDEX_APPMETRICA"]["DELTA"])*(period-1)+1)).strftime('%Y-%m-%d')
    response = ''
# отправляем один и тот же запрос до получения ответа/данных
    while response[:50].find('data') == -1:
        r = requests.get("https://api.appmetrica.yandex.ru/logs/v1/export/events.json?application_id=" + config["YANDEX_APPMETRICA"]["APPLICATION_ID"] + "&fields=" + fields + "&date_since=" + date_since + "&date_until=" + date_until,
            headers = {'Authorization':'OAuth ' + config["YANDEX_APPMETRICA"]["ACCESS_TOKEN"]})
        response = r.text
# приводим полученный ответ к датафрейму
    data = pd.DataFrame(r.json()['data'])
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["application_id", "event_receive_timestamp", "event_timestamp", "session_id", "appmetrica_device_id", "mcc", "mnc", "app_build_number"]:
            data[col] = data[col].fillna("").replace("",0).replace("false","0").replace("true","1").astype(np.int64)
# приведение дат
        elif col in ["event_datetime", "event_receive_datetime"]:
            data[col] = pd.to_datetime(data[col].fillna("2000-01-01 00:00:00"))
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
# добавляем метку времени
        data["ts"] = pd.DatetimeIndex(data["event_datetime"]).asi8
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["YANDEX_APPMETRICA"]["TABLE_EVENTS"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["YANDEX_APPMETRICA"]["TABLE_EVENTS"], con=engine, if_exists='append', chunksize=100)
            except Exception E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_APPMETRICA"]["TABLE_EVENTS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (date_since + "=>" + date_until + ": " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
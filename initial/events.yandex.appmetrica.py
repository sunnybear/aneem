# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import requests
import time
import numpy as np

# импорт библиотек для работы с БД
import mysql.connector as db_connector
# import psycopg2 as db_connector
# import mariadb as db_connector

# импорт настроек
import configparser
config = configparser.ConfigParser()
config.read("../settings.ini")

# создание подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB"]:
    connection = db_connector.connect(
      host=config["DB"]["HOST"],
      user=config["DB"]["USER"],
      password=config["DB"]["PASSWORD"],
      database=config["DB"]["DB"]
    )
    cursor = connection.cursor()

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
            data[col] = data[col].fillna("").replace("",0).replace("false","0").replace("true","1").astype(np.uint64)
# приведение дат
        elif col in ["event_datetime", "event_receive_datetime"]:
            data[col] = pd.to_datetime(data[col].fillna("2000-01-01 00:00:00"))
# приведение строк
        else:
            data[col] = data[col].fillna('')
# добавляем метку времени
    data["ts"] = pd.DatetimeIndex(data["event_datetime"]).asi8
# создаем таблицу в первый раз
    if period == int(["YANDEX_APPMETRICA"]["PERIODS"]):
        cursor.execute((pd.io.sql.get_schema(data, config["YANDEX_APPMETRICA"]["TABLE"])).replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "))
        connection.commit()
    data.to_sql(name=config["YANDEX_APPMETRICA"]["TABLE"], con=connection, if_exists='append')
    connection.commit()

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB"]:
    cursor.close()
    connection.close()
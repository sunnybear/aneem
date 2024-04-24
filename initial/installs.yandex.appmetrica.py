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
    fields = "application_id,attributed_touch_type,click_datetime,click_id,click_ipv6,click_timestamp,click_url_parameters,click_user_agent,profile_id,publisher_id,publisher_name,tracker_name,tracking_id,install_datetime,install_ipv6,install_receive_datetime,install_receive_timestamp,install_timestamp,is_reattribution,is_reinstallation,match_type,appmetrica_device_id,city,connection_type,country_iso_code,device_locale,device_manufacturer,device_model,device_type,google_aid,oaid,ios_ifa,ios_ifv,mcc,mnc,operator_name,os_name,os_version,windows_aid,app_package_name,app_version_name"
    date_since = (date.today() - timedelta(days=int(config["YANDEX_APPMETRICA"]["DELTA"])*period)).strftime('%Y-%m-%d')
    date_until = (date.today() - timedelta(days=int(config["YANDEX_APPMETRICA"]["DELTA"])*(period-1)+1)).strftime('%Y-%m-%d')
    response = ''
# отправляем один и тот же запрос до получения ответа/данных
    while response[:50].find('data') == -1:
        r = requests.get("https://api.appmetrica.yandex.ru/logs/v1/export/installations.json?application_id=" + config["YANDEX_APPMETRICA"]["APPLICATION_ID"] + "&fields=" + fields + "&date_since=" + date_since + "&date_until=" + date_until,
            headers = {'Authorization':'OAuth ' + config["YANDEX_APPMETRICA"]["ACCESS_TOKEN"]})
        response = r.text
# приводим полученный ответ к датафрейму
    data = pd.DataFrame(r.json()['data'])
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["application_id", "click_timestamp", "publisher_id", "tracking_id", "install_receive_timestamp", "install_timestamp", "is_reattribution", "is_reinstallation", "appmetrica_device_id", "mcc", "mnc"]:
            data[col] = data[col].fillna("").replace("",0).replace("false","0").replace("true","1").astype(np.uint64)
# приведение дат
        elif col in ["click_datetime", "install_datetime", "install_receive_datetime"]:
            data[col] = pd.to_datetime(data[col].fillna("2000-01-01 00:00:00"))
# приведение строк
        else:
            data[col] = data[col].fillna('')
# добавляем метку времени
    data["ts"] = pd.DatetimeIndex(data["install_datetime"]).asi8
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
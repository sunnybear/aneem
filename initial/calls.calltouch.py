# Скрипт для первоначального получения журнала звонков Calltouch
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * CALLTOUCH.KEY - Secret Key из настроек аккаунта
# * CALLTOUCH.SITEID - SiteId из настроек аккаунта
# * CALLTOUCH.DELTA - продолжительность периода (в днях) каждой отдельной выгрузки (запроса к API)
# * CALLTOUCH.PERIODS - количество периодов (всех выгрузок), будут выгружены данные за DELTA*PERIODS дней
# * CALLTOUCH.TABLE_CALLS - имя результирующей таблицы для статистики звонков

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
import time
from sqlalchemy import create_engine, text
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# Скрытие предупреждения Unverified HTTPS request
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
# Скрытие предупреждение про fillna
try:
    pd.set_option("future.no_silent_downcasting", True)
except Exception as E:
    pass

# импорт настроек
import configparser
config = configparser.ConfigParser()
config.read("../settings.ini")

# подключение к БД
if config["DB"]["TYPE"] == "MYSQL":
    engine = create_engine('mysql+mysqldb://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "POSTGRESQL":
    engine = create_engine('postgresql+psycopg2://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + '/' + config["DB"]["DB"] + '?client_encoding=utf8')
elif config["DB"]["TYPE"] == "MARIADB":
    engine = create_engine('mariadb+mysqldb://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "ORACLE":
    engine = create_engine('oracle+pyodbc://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + '/' + config["DB"]["DB"])
elif config["DB"]["TYPE"] == "SQLITE":
    engine = create_engine('sqlite:///' + config["DB"]["DB"])

# создание подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection = engine.connect()
    if config["DB"]["TYPE"] in ["MYSQL", "MARIADB"]:
        connection.execute(text('SET NAMES utf8mb4'))
        connection.execute(text('SET CHARACTER SET utf8mb4'))
        connection.execute(text('SET character_set_connection=utf8mb4'))

# создаем таблицу для данных при наличии каких-либо данных
table_not_created = True
data = pd.DataFrame()
# выгружаем данные за 5 лет по месяцам
for period in range(int(config["CALLTOUCH"]["PERIODS"]), 0, -1):
    date_since = (date.today() - timedelta(days=period*int(config["CALLTOUCH"]["DELTA"]))).strftime('%d/%m/%Y')
    date_until = (date.today() - timedelta(days=(period-1)*int(config["CALLTOUCH"]["DELTA"])+1)).strftime('%d/%m/%Y')
# Создание запроса на выгрузку данных (помесячно)
    res_calls = requests.get('https://api.calltouch.ru/calls-service/RestAPI/' + config["CALLTOUCH"]["SITEID"] + '/calls-diary/calls?clientApiId=' + config["CALLTOUCH"]["KEY"] + '&dateFrom=' + date_since + '&dateTo=' + date_until + '&page=1&limit=10000&attribution=0')
# Перезапрос в случае сбоя
    if len(res_calls.text) < 10:
        res_calls = requests.get('https://api.calltouch.ru/calls-service/RestAPI/' + config["CALLTOUCH"]["SITEID"] + '/calls-diary/calls?clientApiId=' + config["CALLTOUCH"]["KEY"] + '&dateFrom=' + date_since + '&dateTo=' + date_until + '&page=1&limit=10000&attribution=0')
# формируем датафрейм из ответа API
    data = pd.concat([data, pd.DataFrame(res_calls.json()['records'])])
    print (date_since + "=>" + date_until + ": " + str(len(data)))

if len(data):
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["callId", "attribution", "duration", "callerNumber", "redirectNumber", "phoneNumber", "siteId", "ctClientId", "successful", "uniqueCall", "targetCall", "uniqTargetCall", "callbackCall", "timestamp"]:
            data[col] = data[col].replace("undefined", "0").replace("Anonymous", "0").fillna(0).astype(np.uint64)
# приведение вещественных чисел
        elif col in ["waitingConnect"]:
            data[col] = data[col].fillna(0.0).astype(float)
# приведение списков
        elif col in ["additionalTags", "orders"]:
            data[col] = data[col].apply(lambda x:'#'.join(x)).fillna('')
# приведение дат
        elif col in ["date", "sessionDate"]:
            data[col] = pd.to_datetime(data[col].fillna("01/01/2000 00:00:00").apply(lambda x: dt.strptime(x, "%d/%m/%Y %H:%M:%S")))
# приведение строк
        else:
            data[col] = data[col].fillna('')
if len(data):
# добавляем метку времени
    data["ts"] = pd.DatetimeIndex(data["date"]).asi8
# создаем таблицу в первый раз
    if table_not_created:
        if config["DB"]["TYPE"] == "CLICKHOUSE":
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["CALLTOUCH"]["TABLE_CALLS"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
        table_not_created = False
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
        try:
            data.to_sql(name=config["CALLTOUCH"]["TABLE_CALLS"], con=engine, if_exists='append', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
        csv_file = data.to_csv().encode('utf-8')
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["CALLTOUCH"]["TABLE_CALLS"] + ' FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["CALLTOUCH"]["TABLE_CALLS"] + " ADD INDEX dateidx (`date`)"))
    connection.execute(text("ALTER TABLE " + config["CALLTOUCH"]["TABLE_CALLS"] + " ADD INDEX callerNumber (`callerNumber`)"))
    connection.execute(text("ALTER TABLE " + config["CALLTOUCH"]["TABLE_CALLS"] + " ADD INDEX yaClientId (`yaClientId`)"))
    connection.execute(text("ALTER TABLE " + config["CALLTOUCH"]["TABLE_CALLS"] + " ADD INDEX utmSource (`utmSource`)"))
    connection.execute(text("ALTER TABLE " + config["CALLTOUCH"]["TABLE_CALLS"] + " ADD INDEX utmMedium (`utmMedium`)"))
    connection.execute(text("ALTER TABLE " + config["CALLTOUCH"]["TABLE_CALLS"] + " ADD INDEX utmCampaign (`utmCampaign`)"))
    connection.commit()
    connection.close()
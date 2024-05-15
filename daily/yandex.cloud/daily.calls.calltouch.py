# Скрипт для ежедневного получения журнала звонков Calltouch
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * CALLTOUCH_KEY - Secret Key из настроек аккаунта
# * CALLTOUCH_SITEID - SiteId из настроек аккаунта
# * CALLTOUCH_TABLE_CALLS - имя результирующей таблицы для статистики звонков

# requirements.txt:
# pandas
# numpy
# requests
# datetime
# sqlalchemy

# timeout: 300
# memory: 256

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
import time
import os
from sqlalchemy import create_engine, text
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# Скрытие предупреждения Unverified HTTPS request
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
# Скрытие предупреждение про fillna
try:
    pd.set_option("future.no_silent_downcasting", True)
except Exception as E:
    pass

def handler(event, context):
    auth = {
        'X-ClickHouse-User': os.getenv('DB_USER'),
        'X-ClickHouse-Key': context.token["access_token"]
    }
    auth_post = auth.copy()
    auth_post['Content-Type'] = 'application/octet-stream'
    cacert = '/etc/ssl/certs/ca-certificates.crt'
    yesterday = (date.today() - timedelta(days=1)).strftime('%d/%m/%Y')

# подключение к БД
    if os.getenv('DB_TYPE') == "MYSQL":
        engine = create_engine('mysql+mysqldb://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB') + '?charset=utf8')
    elif os.getenv('DB_TYPE') == "POSTGRESQL":
        engine = create_engine('postgresql+psycopg2://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB') + '?client_encoding=utf8')
    elif os.getenv('DB_TYPE') == "MARIADB":
        engine = create_engine('mariadb+mysqldb://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB') + '?charset=utf8')
    elif os.getenv('DB_TYPE') == "ORACLE":
        engine = create_engine('oracle+pyodbc://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB'))
    elif os.getenv('DB_TYPE') == "SQLITE":
        engine = create_engine('sqlite:///' + os.getenv('DB_DB'))

# создание подключения к БД
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection = engine.connect()
        if os.getenv('DB_TYPE') in ["MYSQL", "MARIADB"]:
            connection.execute(text('SET NAMES utf8mb4'))
            connection.execute(text('SET CHARACTER SET utf8mb4'))
            connection.execute(text('SET character_set_connection=utf8mb4'))

# формируем запрос для получения списка звонков за вчера
    calls = requests.get('https://api.calltouch.ru/calls-service/RestAPI/' + os.getenv('CALLTOUCH_SITEID') + '/calls-diary/calls?clientApiId=' + os.getenv('CALLTOUCH_KEY') + '&dateFrom=' + yesterday + '&dateTo=' + yesterday + '&page=1&limit=10000&attribution=0').json()

# преобразуем список в датафрейм
    data = pd.DataFrame(calls["records"])
    del calls
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
# удаление старых данных
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            try:
                connection.execute(text("DELETE FROM " + os.getenv('CALLTOUCH_TABLE_CALLS') + " WHERE `date`>='" + yesterday + "'"))
                connection.commit()
            except Exception as E:
                print (E)
                connection.rollback()
        elif os.getenv('DB_TYPE') == "CLICKHOUSE":
# удаление старых данных
            requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth, verify=cacert,
                params={"database": os.getenv('DB_DB'), "query": "DELETE FROM " + os.getenv('DB_PREFIX') + "." + os.getenv('CALLTOUCH_TABLE_CALLS') + " WHERE `date`>='" + yesterday + "'"})
# добавление новых данных
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            try:
                data.to_sql(name=os.getenv('CALLTOUCH_TABLE_CALLS'), con=engine, if_exists='append', chunksize=100)
                connection.commit()
            except Exception as E:
                print (E)
                connection.rollback()
        elif os.getenv('DB_TYPE') == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + os.getenv('DB_HOST') + ':8443/?database=' + os.getenv('DB_DB') + '&query=INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('CALLTOUCH_TABLE_CALLS') + ' FORMAT CSV',
                headers=auth_post, data=csv_file, stream=True)
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection.close()

    return {
        'statusCode': 200,
        'body': "LoadedCalls: " + str(len(data))
    }
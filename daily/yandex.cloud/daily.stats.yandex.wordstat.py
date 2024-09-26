# Скрипт для ежедневного сбора статистики поисковых запросов Яндекс.Wordstat (за последний месяц)
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * YANDEX_WORDSTAT_ACCESS_TOKEN - Access Token для приложения, имеющего доступ к Яндекс.Директ
# * YANDEX_WORDSTAT_PHRASES - список фраз через запятую (можно использовать спецсимволы + и -)
# * YANDEX_WORDSTAT_GEO - список регионов для сбора статистики (пусто - все регионы), https://word-keeper.ru/kody-regionov-yandeksa
# * YANDEX_WORDSTAT_GEO_SEPARATE - собирать статистику по каждому региону в отдельности (=1) или все вместе (=0)
# * YANDEX_WORDSTAT_TABLE_SHOWS_DAILY - имя результирующей таблицы для ежедневной статистики запросов

# requirements.txt:
# pandas
# numpy
# requests
# datetime
# sqlalchemy

# timeout: 300
# memory: 512

import json
import pandas as pd
import numpy as np
import os
import io
import requests
from datetime import datetime as dt
from datetime import date, timedelta
import time
from sqlalchemy import create_engine, text

def handler(event, context):
    auth = {
        'X-ClickHouse-User': os.getenv('DB_USER'),
        'X-ClickHouse-Key': context.token["access_token"]
    }
    auth_post = auth.copy()
    auth_post['Content-Type'] = 'application/octet-stream'
    cacert = '/etc/ssl/certs/ca-certificates.crt'
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    month_ago = pd.to_datetime(date.today()) - pd.DateOffset(months=1)
    month_days = (month_ago.replace(month=month_ago.month%12 + 1, day=1) - timedelta(days=1)).day

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

# нужно ли удалить все данные за вчера
    data_not_cleaned = True
# токен Яндекс.Метрики
    TOKEN = os.getenv('YANDEX_WORDSTAT_ACCESS_TOKEN')
    API_ENDPOINT = 'https://api-sandbox.direct.yandex.ru/v4/json/'
# список ключевых слов
    phrases = os.getenv('YANDEX_WORDSTAT_PHRASES').split(",")
# группы ключевых слов (по 10) для одновременного запроса Wordstat
    phrases_groups = np.array_split(np.array(phrases), int(len(phrases)/10) + np.sign(len(phrases)%10))
# география, приведенная к целым
    if os.getenv('YANDEX_WORDSTAT_GEO_SEPARATE') == "1":
        geos = [[int(x)] for x in os.getenv('YANDEX_WORDSTAT_GEO').split(",")]
    else:
        geos = [[int(x) for x in os.getenv('YANDEX_WORDSTAT_GEO').split(",")]]
# собираем статистику по фразам и географии
    items = []
    for geo in geos:
        for phrases_group in phrases_groups:
# создаем отчет
            data_report = {
                'method': 'CreateNewWordstatReport',
                'token': TOKEN,
                'param': {'Phrases': list(phrases_group), 'GeoID': geo}
            }
            data_report = json.dumps(data_report, ensure_ascii=False).encode('utf-8')
            report_id = requests.post(API_ENDPOINT, data_report).json()
            time.sleep(5)
# проверяем статус отчета
            data_check = {
                'method': 'GetWordstatReport',
                'token': TOKEN,
                'param': int(report_id['data'])
            }
            data_check = json.dumps(data_check, ensure_ascii=False).encode('utf-8')
            report_data = requests.post(API_ENDPOINT, data_check).json()
# переотправляям запрос на готовность отчета, пока не получим ответ
            while 'data' not in report_data:
                time.sleep(5)
                report_data = requests.post(API_ENDPOINT, data_check).json()
# удаляем отчет
            data_delete = {
                'method': 'DeleteWordstatReport',
                'token': TOKEN,
                'param': int(report_id['data'])
            }
            data_delete = json.dumps(data_delete, ensure_ascii=False).encode('utf-8')
            requests.post(API_ENDPOINT, data_delete).json()
# разбираем данные
            for i, result in enumerate(report_data['data']):
                for result_item in result['SearchedWith']:
                    if result_item['Phrase'] == phrases_group[i]:
                        items.append([yesterday, result_item['Phrase'], int(round(result_item['Shows']/month_days)), result_item['Shows'], ','.join([str(x) for x in geo])])
    data = pd.DataFrame(items, columns=['Date', 'Phrase', 'Shows', 'ShowsMonth', 'Geo'])
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["Shows", "ShowsMonth"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение дат
        elif col in ["Date"]:
            data[col] = pd.to_datetime(data[col].apply(lambda x: dt.strptime(x, "%Y-%m-%d")))
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
# добавляем метку времени
        data["ts"] = pd.DatetimeIndex(data["Date"]).asi8
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            if data_not_cleaned:
# обновление данных о визитах
                try:
                    connection.execute(text("DELETE FROM " + os.getenv('YANDEX_WORDSTAT_TABLE_SHOWS_DAILY') + " WHERE `Date`>='" + yesterday + "'"))
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
            try:
                data.to_sql(name=os.getenv('YANDEX_WORDSTAT_TABLE_SHOWS_DAILY'), con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif os.getenv('DB_TYPE') == "CLICKHOUSE":
# создаем таблицу, если в первый раз
            requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                params={"database": os.getenv('DB_DB'), "query": (pd.io.sql.get_schema(data, os.getenv('YANDEX_WORDSTAT_TABLE_SHOWS_DAILY')) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE " + os.getenv('DB_DB') + ".").replace("INTEGER", "Int64")})
# удаляем данные за вчера
            if data_not_cleaned:
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": "DELETE FROM " + os.getenv('DB_PREFIX') + "." + os.getenv('YANDEX_WORDSTAT_TABLE_SHOWS_DAILY') + " WHERE `Date`>='" + yesterday + "'"})
# добавляем новые данные
            csv_file = data.to_csv(index=False).encode('utf-8')
            requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                params={"database": os.getenv('DB_DB'), "query": 'INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('YANDEX_WORDSTAT_TABLE_SHOWS_DAILY') + ' FORMAT CSV'},
                data=csv_file, stream=True)
        data_not_cleaned = False
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection.close()

    return {
        'statusCode': 200,
        'body': "LoadedStats: " + str(len(data))
    }
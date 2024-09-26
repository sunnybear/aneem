# Скрипт для ежедневного сбора статистики поисковых запросов Яндекс.Wordstat (за последний месяц)
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * YANDEX_WORDSTAT.ACCESS_TOKEN - Access Token для приложения, имеющего доступ к Яндекс.Директ
# * YANDEX_WORDSTAT.PHRASES - список фраз через запятую (можно использовать спецсимволы + и -)
# * YANDEX_WORDSTAT.GEO - список регионов для сбора статистики (пусто - все регионы), https://word-keeper.ru/kody-regionov-yandeksa
# * YANDEX_WORDSTAT.GEO_SEPARATE - собирать статистику по каждому региону в отдельности (=1) или все вместе (=0)
# * YANDEX_WORDSTAT.TABLE_SHOWS_DAILY - имя результирующей таблицы для ежедневной статистики запросов

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import requests
import time
import numpy as np
import json
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
config.read("../../settings.ini", encoding='utf-8')

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

yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
month_ago = pd.to_datetime(date.today()) - pd.DateOffset(months=1)
month_days = (month_ago.replace(month=month_ago.month%12 + 1, day=1) - timedelta(days=1)).day
# метка очистки старых данных
data2clean = True
# токен Яндекс.Метрики
TOKEN = config["YANDEX_WORDSTAT"]["ACCESS_TOKEN"]
API_ENDPOINT = 'https://api-sandbox.direct.yandex.ru/v4/json/'
# список ключевых слов
phrases = config["YANDEX_WORDSTAT"]["PHRASES"].split(",")
# группы ключевых слов (по 10) для одновременного запроса Wordstat
phrases_groups = np.array_split(np.array(phrases), int(len(phrases)/10) + np.sign(len(phrases)%10))
# география, приведенная к целым
if config["YANDEX_WORDSTAT"]["GEO_SEPARATE"] == "1":
    geos = [[int(x)] for x in config["YANDEX_WORDSTAT"]["GEO"].split(",")]
else:
    geos = [[int(x) for x in config["YANDEX_WORDSTAT"]["GEO"].split(",")]]
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
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обновление статистики
        if data2clean:
            try:
                connection.execute(text("DELETE FROM " + config["YANDEX_WORDSTAT"]["TABLE_SHOWS_DAILY"] + " WHERE `Date`>='" + yesterday + "'"))
                connection.commit()
            except Exception as E:
                print (E)
                connection.rollback()
        try:
            data.to_sql(name=config["YANDEX_WORDSTAT"]["TABLE_SHOWS_DAILY"], con=engine, if_exists='append', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
# создаем таблицу, если в первый раз
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
            params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["YANDEX_WORDSTAT"]["TABLE_SHOWS_DAILY"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
        if data2clean:
# удаляем данные за сегодня
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": "DELETE FROM " + config["DB"]["DB"] + "." + config["YANDEX_WORDSTAT"]["TABLE_SHOWS_DAILY"] + " WHERE `Date`>='" + yesterday + "'"}, headers={'Content-Type':'application/octet-stream'}, verify=False)
# добавляем новые данные
        csv_file = data.to_csv(index=False).encode('utf-8')
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_WORDSTAT"]["TABLE_SHOWS_DAILY"] + ' FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    data2clean = False
print (yesterday + ": " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
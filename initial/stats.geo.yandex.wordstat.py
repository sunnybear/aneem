# Скрипт для разовой выгрузки регионов Yandex.Wordstat
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * YANDEX_WORDSTAT.ACCESS_TOKEN - Access Token для приложения, имеющего доступ к Яндекс.Директ
# * YANDEX_WORDSTAT.TABLE_GEO - имя результирующей таблицы для списка регионов с ID

# импорт общих библиотек
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
config.read("../settings.ini", encoding='utf-8')

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

# токен Яндекс.Метрики
TOKEN = config["YANDEX_WORDSTAT"]["ACCESS_TOKEN"]
API_ENDPOINT = 'https://api-sandbox.direct.yandex.ru/v4/json/'
# создаем запрос
data_report = {
    'method': 'GetRegions',
    'token': TOKEN
}
data_report = json.dumps(data_report, ensure_ascii=False).encode('utf-8')
report_geo = requests.post(API_ENDPOINT, data_report).json()
data = []
if 'data' in report_geo:
    data = pd.DataFrame(report_geo['data'])
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["RegionID", "ParentID"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение строк
        else:
            data[col] = data[col].fillna('')
if len(data):
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        try:
            data.to_sql(name=config["YANDEX_WORDSTAT"]["TABLE_GEO"], con=engine, if_exists='replace', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
# создаем таблицу
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
            params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["YANDEX_WORDSTAT"]["TABLE_GEO"]) + "  ENGINE=MergeTree ORDER BY (`RegionID`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
# добавляем данные
        csv_file = data.to_csv(index=False).encode('utf-8')
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_WORDSTAT"]["TABLE_GEO"] + ' FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
print ("Yandex.Wordstat Geo: " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# создаем индексы
    connection.execute(text("ALTER TABLE " + config["YANDEX_WORDSTAT"]["TABLE_GEO"] + " ADD INDEX idx_regionid (`RegionID`)"))
    connection.execute(text("ALTER TABLE " + config["YANDEX_WORDSTAT"]["TABLE_GEO"] + " ADD INDEX idx_parentid (`ParentID`)"))
    connection.commit()
    connection.close()
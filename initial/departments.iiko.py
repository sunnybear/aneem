# Скрипт для первоначального получения всех данных по департаментам из Iiko
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.PORT - порт хоста базы данных (если отличается от стандартного)
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * IIKO.API_ENDPOINT - точка доступа API
# * IIKO.ACCESS_TOKEN_LOGIN - логин для получения Access Token
# * IIKO.ACCESS_TOKEN_PASS - логин для получения Access Token
# * IIKO.TABLE_DEPARTMENTS - имя результирующей таблицы для департаментов (точек)

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
import time
import xmltodict
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
if config["DB"]["PORT"] != '':
    DB_PORT = ':' + config["DB"]["PORT"]
else:
    DB_PORT = ''
if config["DB"]["TYPE"] == "MYSQL":
    engine = create_engine('mysql+mysqldb://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "POSTGRESQL":
    engine = create_engine('postgresql+psycopg2://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"] + '?client_encoding=utf8')
elif config["DB"]["TYPE"] == "MARIADB":
    engine = create_engine('mariadb+mysqldb://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "ORACLE":
    engine = create_engine('oracle+pyodbc://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"])
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
token_timestamp = time.time()
TOKEN = ''

# получение временного токена
result_token = requests.get(config['IIKO']['API_ENDPOINT'] + '/resto/api/auth?login=' + config['IIKO']['ACCESS_TOKEN_LOGIN'] + '&pass=' + config['IIKO']['ACCESS_TOKEN_PASS'])
TOKEN = result_token.text
token_timestamp = time.time()

# отправка основного запроса
result = xmltodict.parse(requests.get(config['IIKO']['API_ENDPOINT'] + '/resto/api/corporation/departments/',
    headers = {'Cookie': 'key=' + TOKEN, 'Accept-Type': 'application/json'}).content)

data = pd.DataFrame()
if len(result):
    departments = {}
    for item in result['corporateItemDtoes']['corporateItemDto']:
        if item['type'] == 'DEPARTMENT':
            department = {}
            for k in item.keys():
                department[k] = item[k]
            departments[department['id']] = department
        elif item['type'] == 'JURPERSON':
            for d in departments.keys():
                if departments[d]['taxpayerIdNumber'] == item['jurPersonAdditionalPropertiesDto']['taxpayerId']:
                    departments[d]['jurname'] = item['name']
                    departments[d]['address'] = item['jurPersonAdditionalPropertiesDto']['address']
# формируем датафрейм из ответа API
    data = pd.DataFrame.from_dict(departments, orient='index')
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение строк
        data[col] = data[col].fillna('')

if len(data):
# поддержка TCP HTTP для Clickhouse
    if config["DB"]["PORT"] != '8443':
        CLICKHOUSE_PROTO = 'http://'
        CLICKHOUSE_PORT = config["DB"]["PORT"]
    else:
        CLICKHOUSE_PROTO = 'https://'
        CLICKHOUSE_PORT = config["DB"]["PORT"]
# создаем таблицу в первый раз
    if table_not_created:
        if config["DB"]["TYPE"] == "CLICKHOUSE":
            requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/', verify=False,
                params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["IIKO"]["TABLE_DEPARTMENTS"]) + "  ENGINE=MergeTree ORDER BY (`id`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
        table_not_created = False
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
        try:
            data.to_sql(name=config["IIKO"]["TABLE_DEPARTMENTS"], con=engine, if_exists='append', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
        csv_file = data.to_csv(index=False, header=False).encode('utf-8')
        r = requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["IIKO"]["TABLE_DEPARTMENTS"] + ' FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("Departments:", len(data))
else:
    print ("No departments")

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["IIKO"]["TABLE_DEPARTMENTS"] + " ADD INDEX id (`id`)"))
    connection.commit()
    connection.close()
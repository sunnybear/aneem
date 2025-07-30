# Скрипт для первоначального получения всех покупателей из Iiko.web по номерам карт/телефонов
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.PORT - порт хоста базы данных (если отличается от стандартного)
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * IIKOWEB.ACCESS_TOKEN - токен доступа к https://api-ru.iiko.services/
# * IIKOWEB.CUSTOMER_CARDS_RANGES - список начальных диапазонов карт пользователей, через запятую в формате начало_диапазона:длина. Например, XXXXXXXXXXXXXXX:10000
# * IIKOWEB.CUSTOMER_CARDS - список карт пользователей (не из диапазонов), через запятую
# * IIKOWEB.TABLE_CUSTOMERS - имя результирующей таблицы для покупателей

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
if 'PORT' in config["DB"] and config["DB"]["PORT"] != '':
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
customers = []

# отправка запроса на временный токен
try:
    auth_result = requests.post('https://api-ru.iiko.services/api/1/access_token', json={'apiLogin': config['IIKOWEB']['ACCESS_TOKEN']}).json()
except Exception:
    auth_result = {}
if 'token' in auth_result:
    TOKEN = auth_result['token']
    org_result = requests.get('https://api-ru.iiko.services/api/1/organizations', headers={'Authorization': 'Bearer ' + TOKEN})
    org_id = org_result.json()['organizations'][0]['id']
# перебираем диапазоны с картами
    ranges = []
    if 'CUSTOMER_CARDS_RANGES' in config['IIKOWEB']:
        ranges = config['IIKOWEB']['CUSTOMER_CARDS_RANGES'].split(",")
    for range_ in ranges:
        range_ = range_.split(':')
        for i in range(int(range_[1])):
            customer_card = str(int(range_[0]) + i)
# получаем ID покупателя по номеру карты
            try:
                customer_result = requests.post('https://api-ru.iiko.services/api/1/loyalty/iiko/customer/info', json={'organizationId': org_id, 'type': 'cardNumber', 'cardNumber': customer_card}, headers={'Authorization': 'Bearer ' + TOKEN}).json()
            except Exception:
                customer_result = {}
            if 'id' in customer_result:
                customer_id = customer_result['id']
                customers.append({'customerId': customer_id, 'customerCard': customer_card, 'organizationId': org_id})
# выгружаем карты по одной
    customer_cards = []
    if 'CUSTOMER_CARDS' in config['IIKOWEB']:
        customer_cards = config['IIKOWEB']['CUSTOMER_CARDS'].split(",")
    for customer_card in customer_cards:
        customer_card = customer_card.strip()
# получаем ID покупателя по номеру карты
        try:
            customer_result = requests.post('https://api-ru.iiko.services/api/1/loyalty/iiko/customer/info', json={'organizationId': org_id, 'type': 'cardNumber', 'cardNumber': customer_card}, headers={'Authorization': 'Bearer ' + TOKEN}).json()
        except Exception:
            customer_result = {}
# пробуем по номеру телефона
        if 'id' not in customer_result:
            try:
                customer_result = requests.post('https://api-ru.iiko.services/api/1/loyalty/iiko/customer/info', json={'organizationId': org_id, 'type': 'phone', 'phone': customer_card}, headers={'Authorization': 'Bearer ' + TOKEN}).json()
            except Exception:
                customer_result = {}
# пробуем по номеру телефона 7
        if 'id' not in customer_result:
            try:
                customer_result = requests.post('https://api-ru.iiko.services/api/1/loyalty/iiko/customer/info', json={'organizationId': org_id, 'type': 'phone', 'phone': '7' +customer_card}, headers={'Authorization': 'Bearer ' + TOKEN}).json()
            except Exception:
                customer_result = {}
# пробуем по номеру телефона +7
        if 'id' not in customer_result:
            try:
                customer_result = requests.post('https://api-ru.iiko.services/api/1/loyalty/iiko/customer/info', json={'organizationId': org_id, 'type': 'phone', 'phone': '+7' +customer_card}, headers={'Authorization': 'Bearer ' + TOKEN}).json()
            except Exception:
                customer_result = {}
        if 'id' in customer_result:
            customer_id = customer_result['id']
            customers.append({'customerId': customer_id, 'customerCard': customer_card, 'organizationId': org_id})
        else:
            print (customer_result)
    data = pd.DataFrame(customers)
    if len(data):
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение строк
            data[col] = data[col].fillna('')
# поддержка TCP HTTP для Clickhouse
        if 'PORT' in config["DB"] and config["DB"]["PORT"] != '8443':
            CLICKHOUSE_PROTO = 'http://'
            CLICKHOUSE_PORT = config["DB"]["PORT"]
        else:
            CLICKHOUSE_PROTO = 'https://'
            CLICKHOUSE_PORT = '8443'
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["IIKOWEB"]["TABLE_CUSTOMERS"]) + "  ENGINE=MergeTree ORDER BY (`customerId`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["IIKOWEB"]["TABLE_CUSTOMERS"], con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv(index=False).encode('utf-8')
            requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["IIKOWEB"]["TABLE_CUSTOMERS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
        print ("Customers:", len(data))
else:
    print ('Ошибка доступа')

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["IIKO"]["TABLE_ORDERS"] + " ADD INDEX customerids (`customerId`)"))
    connection.commit()
    connection.close()
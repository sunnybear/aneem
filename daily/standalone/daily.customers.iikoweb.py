# Скрипт для регулярного получения новых покупателей из Iiko.web по номерам карт
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.PORT - порт хоста базы данных (если отличается от стандартного)
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * IIKOWEB.ACCESS_TOKEN - токен доступа к https://api-ru.iiko.services/
# * IIKOWEB.CUSTOMER_CARDS_RANGES - список начальных диапазонов карт пользователей, через запятую в формате начало_диапазона:длина. Например, XXXXXXXXXXXXXXX:10000
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
# поддержка TCP HTTP для Clickhouse
    if 'PORT' in config["DB"] and config["DB"]["PORT"] != '8443':
        CLICKHOUSE_PROTO = 'http://'
        CLICKHOUSE_PORT = config["DB"]["PORT"]
    else:
        CLICKHOUSE_PROTO = 'https://'
        CLICKHOUSE_PORT = '8443'
# перебираем диапазоны с картами
    ranges = config['IIKOWEB']['CUSTOMER_CARDS_RANGES'].split(",")
    for range_ in ranges:
        range_ = range_.split(':')
# получаем максимальный номер последней карты в диапазоне
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            customer_card_max = int(pd.read_sql("SELECT max(CAST(customerCard AS INT64)) AS c FROM " + config["DB"]["DB"] + "." + config["IIKOWEB"]["TABLE_CUSTOMERS"] + " WHERE CAST(customerCard AS INT64)>" + range_[0] + " and CAST(customerCard AS INT64)/" + range_[0] + "<2", connection)["c"].values[0])
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            customer_card_max = requests.get(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/', verify=False,
            params={"database": config["DB"]["DB"], "query": 'SELECT max(toInt64(customerCard)) FROM ' + config["DB"]["DB"] + '.' + config["IIKOWEB"]["TABLE_CUSTOMERS"] + " WHERE toInt64(customerCard)>" + range_[0] + " AND toInt64(customerCard)/" + range_[0] + "<2"})
        for i in range(1, 5000):
            customer_card = str(customer_card_max + i)
# получаем ID покупателя по номеру карты
            customer_result = requests.post('https://api-ru.iiko.services/api/1/loyalty/iiko/customer/info', json={'organizationId': org_id, 'type': 'cardNumber', 'cardNumber': customer_card}, headers={'Authorization': 'Bearer ' + TOKEN}).json()
            if 'id' in customer_result:
                customer_id = customer_result['id']
                customers.append({'customerId': customer_id, 'customerCard': customer_card, 'organizationId': org_id})
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
# Скрипт для первоначального получения транзакций программы лояльности всех покупателей из Iiko.web
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.PORT - порт хоста базы данных (если отличается от стандартного)
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * IIKOWEB.ACCESS_TOKEN - токен доступа к https://api-ru.iiko.services/
# * IIKOWEB.TABLE_CUSTOMERS - имя таблицы со списком покупателей
# * IIKOWEB.TABLE_TRANSACTIONS - имя результирующей таблицы для транзакций

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
config.read("../../settings.ini")

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

transactions = []
date_since = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00.000')
date_until = (date.today()).strftime('%Y-%m-%d 23:59:59.999')

# отправка запроса на временный токен
try:
    auth_result = requests.post('https://api-ru.iiko.services/api/1/access_token', json={'apiLogin': config['IIKOWEB']['ACCESS_TOKEN']}).json()
except Exception:
    auth_result = {}
if 'token' in auth_result:
    TOKEN = auth_result['token']
# поддержка TCP HTTP для Clickhouse
    if 'PORT' in config["DB"] and config["DB"]["PORT"] != '8443':
        CLICKHOUSE_PROTO = 'http://'
        CLICKHOUSE_PORT = config["DB"]["PORT"]
    else:
        CLICKHOUSE_PROTO = 'https://'
        CLICKHOUSE_PORT = '8443'
# получение всех ID объектов
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        ids = list(pd.read_sql("SELECT distinct customerId FROM " + config["DB"]["DB"] + "." + config["IIKOWEB"]["TABLE_CUSTOMERS"], connection)["customerId"].values)
        org_id = pd.read_sql("SELECT distinct organizationId FROM " + config["DB"]["DB"] + "." + config["IIKOWEB"]["TABLE_CUSTOMERS"], connection)["organizationId"].values[0]
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
        ids_req = requests.get(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/', verify=False,
            params={"database": config["DB"]["DB"], "query": 'SELECT distinct customerId FROM ' + config["DB"]["DB"] + '.' + config["IIKOWEB"]["TABLE_CUSTOMERS"]})
        ids = list(ids_req.text.split("\n"))
        org_id_req = requests.get(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/', verify=False,
            params={"database": config["DB"]["DB"], "query": 'SELECT distinct organizationId FROM ' + config["DB"]["DB"] + '.' + config["IIKOWEB"]["TABLE_CUSTOMERS"]})
        org_id = org_id_req.text.split("\n")[0]
    for customer_id in ids:
# получаем все транзации по покупателю
        try:
            transactions_result = requests.post('https://api-ru.iiko.services/api/1/loyalty/iiko/customer/transactions/by_date', json={'organizationId': org_id, 'customerId': customer_id, 'pageSize': 1000, 'pageNumber': 0, 'dateFrom': date_since, 'dateTo': date_until}, headers={'Authorization': 'Bearer ' + TOKEN}).json()
        except Exception:
            transactions_result = {}
        if 'transactions' in transactions_result:
            for item in transactions_result['transactions']:
                item['customerId'] = customer_id
                transactions.append(item)
    data = pd.DataFrame(transactions)
    if len(data):
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["type", "isDelivery", "isIgnored", "revision", "orderNumber"]:
                data[col] = data[col].fillna(0).replace('', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["balanceAfter", "balanceBefore", "posBalanceBefore", "sum", "orderSum"]:
                data[col] = data[col].fillna(0.0).replace('', 0.0).astype(float)
# приведение дат
            elif col in ["whenCreated", "whenCreatedOrder"]:
                data[col] = pd.to_datetime(data[col].str.replace(r'\..*', '', regex=True), format='ISO8601')
# приведение строк
            else:
                data[col] = data[col].fillna('')
# удаление данных
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            try:
                connection.execute(text("DELETE FROM " + config["IIKOWEB"]["TABLE_TRANSACTIONS"] + " WHERE whenCreated>='" + date_since.replace('.000', '') + "'"), con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/',verify=False,
                params={"database": config["DB"]["DB"], "query": 'DELETE FROM ' + config["DB"]["DB"] + '.' + config["IIKOWEB"]["TABLE_TRANSACTIONS"] + " WHERE whenCreated>='" + date_since.replace('.000', '') + "'"})
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["IIKOWEB"]["TABLE_TRANSACTIONS"], con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv(index=False).encode('utf-8')
            requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["IIKOWEB"]["TABLE_TRANSACTIONS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
        print (date_since, "=>", date_until, ":", len(data))
else:
    print ('Ошибка доступа')

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["IIKOWEB"]["TABLE_TRANSACTIONS"] + " ADD INDEX ididx (`id`)"))
    connection.execute(text("ALTER TABLE " + config["IIKOWEB"]["TABLE_TRANSACTIONS"] + " ADD INDEX dateidx (`whenCreated`)"))
    connection.commit()
    connection.close()
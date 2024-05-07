# Скрипт для первоначального получения заказов из интернет-магазина (1С-)Битрикс: sale.order
# Инструкция по получению URL для REST API: https://www.brekot.ru/blog/bitrix-rest-api/ 
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * BITRIX.WEBHOOK - URL вебхука (REST API) Битрикс
# * BITRIX.TABLE_ORDERS - имя результирующей таблицы для sale.order

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
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

# получение количества заказов
orders = requests.get(config["BITRIX"]["WEBHOOK"] + 'sale.order.list').json()
# общее количество заказов
orders_total = int(orders["total"])
# текущий id заказа - для следующего запроса
last_order_id = 0
# счетчик количества заказов
orders_current = 0
# запросы пакетами по 50 заказов до исчерпания количества для загрузки
while orders_current < orders_total:
    orders = {}
    orders_req = requests.get(config["BITRIX"]["WEBHOOK"] + 'sale.order.list?filter[>id]=' + str(last_order_id)).json()
# разбор заказов
    for order in orders_req["result"]["orders"]:
        last_order_id = int(order['id'])
        orders[last_order_id] = order
    orders_current += len(orders)
# формируем датафрейм
    data = pd.DataFrame.from_dict(orders, orient='index')
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["accountNumber", "companyId", "empCanceledId", "empMarkedId", "empStatusId", "id", "userId", "affiliateId", "recurringId"]:
            data[col] = data[col].fillna('').replace('None', '').replace('', 0).astype(np.int64)
# приведение вещественных чисел
        elif col in ["discountValue", "price", "taxValue"]:
# приведение дат
            data[col] = data[col].fillna('').replace('', 0.0).astype(float)
        elif col in ["dateCanceled", "dateInsert", "dateLock", "dateMarked", "dateStatus", "dateUpdate"]:
            data[col] = pd.to_datetime(data[col].fillna('').replace('None', '').replace('', '2000-01-01T00:00:00+03:00').apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%S%z').strftime("%Y-%m-%d %H:%M:%S").replace('202-','2020-')))
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
        data["ts"] = pd.DatetimeIndex(data["dateInsert"]).asi8
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["BITRIX"]["TABLE_ORDERS"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["BITRIX"]["TABLE_ORDERS"], con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["BITRIX"]["TABLE_ORDERS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (str(last_order_id) + ": " + str(orders_current))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# создаем индексы
    connection.execute(text("ALTER TABLE " + config["BITRIX"]["TABLE_ORDERS"] + " ADD INDEX dateinsert (`dateInsert`)"))
	connection.execute(text("ALTER TABLE " + config["BITRIX"]["TABLE_ORDERS"] + " ADD INDEX dateupdate (`dateUpdate`)"))
    connection.execute(text("ALTER TABLE " + config["BITRIX"]["TABLE_ORDERS"] + " ADD INDEX id (`id`)"))
    connection.execute(text("ALTER TABLE " + config["BITRIX"]["TABLE_ORDERS"] + " ADD INDEX statusid (`statusId`)"))
    connection.commit()
    connection.close()
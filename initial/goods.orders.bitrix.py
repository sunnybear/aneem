# Скрипт для первоначального получения товаров по заказам из интернет-магазина (1С-)Битрикс: sale.order.get
# Инструкция по получению URL для REST API: https://www.brekot.ru/blog/bitrix-rest-api/ 
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * BITRIX.WEBHOOK - URL вебхука (REST API) Битрикс
# * BITRIX.TABLE_ORDERS_GOODS - имя результирующей таблицы для sale.order.get

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
last_order_id = int(orders["result"]["orders"][0]['id'])
# счетчик количества заказов
orders_current = 0
# запросы пакетами по 50 заказов до исчерпания количества для загрузки
while orders_current < orders_total:
    orders_goods = []
    cmd = ['cmd[0]=sale.order.get%3Fid%3D' + str(last_order_id)]
    for i in range(1, 50):
        last_order_id += 1
        cmd.append('cmd[' + str(i) + ']=sale.order.get%3Fid%3D' + str(last_order_id))
    orders_req = requests.get(config["BITRIX"]["WEBHOOK"] + 'batch.json?' + '&'.join(cmd)).json()
# разбор заказов
    for order in orders_req["result"]["result"]:
        if "order" in order:
            order = order["order"]
        if "basketItems" in order:
            for item in order["basketItems"]:
                orders_goods.append([order["id"], item["name"], item["price"], item["quantity"]])
        orders_current += 1
# формируем датафрейм
    data = pd.DataFrame(orders_goods)
    if len(data):
        data.columns=["orderId", "name", "price", "quantity"]
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["orderId"]:
                data[col] = data[col].fillna('').replace('None', '').replace('', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["price", "quantity"]:
# приведение дат
                data[col] = data[col].fillna('').replace('', 0.0).astype(float)
# приведение строк
            else:
                data[col] = data[col].fillna('')
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["BITRIX"]["TABLE_ORDERS_GOODS"]) + "  ENGINE=MergeTree ORDER BY (`orderid`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["BITRIX"]["TABLE_ORDERS_GOODS"], con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["BITRIX"]["TABLE_ORDERS_GOODS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (str(last_order_id) + ": " + str(orders_current) + "/" + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# создаем индексы
    connection.execute(text("ALTER TABLE " + config["BITRIX"]["TABLE_ORDERS_GOODS"] + " ADD INDEX orderid (`orderId`)"))
    connection.execute(text("ALTER TABLE " + config["BITRIX"]["TABLE_ORDERS_GOODS"] + " ADD INDEX price (`price`)"))
    connection.execute(text("ALTER TABLE " + config["BITRIX"]["TABLE_ORDERS_GOODS"] + " ADD INDEX quantity (`quantity`)"))
    connection.commit()
    connection.close()
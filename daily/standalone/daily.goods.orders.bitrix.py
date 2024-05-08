# Скрипт для ежедневного заказов из интернет-магазина (1С-)Битрикс: sale.order
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
import requests
import time
from tapi_yandex_metrika import YandexMetrikaLogsapi
import numpy as np
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

# Создание запроса на выгрузку данных (вчера)
yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

# выбираем id всех заказов, которые обновились
orders_next = 50
ids = {}
last_order_id = 0
while orders_next>= 50:
    if last_order_id == 0:
        orders_req = requests.get(config["BITRIX"]["WEBHOOK"] + 'sale.order.list?filter[>dateUpdate]=' + yesterday).json()
    else:
        orders_req = requests.get(config["BITRIX"]["WEBHOOK"] + 'sale.order.list?filter[>id]=' + str(last_order_id)).json()
    for order in orders_req["result"]["orders"]:
        last_order_id = int(order['id'])
        ids.append(last_order_id)
    if int(orders_req["total"]) > 50:
        orders_next = int(orders_req["next"])
    else:
        orders_next = 0
# получаем список товаров для заказов, которые обновились
ids_i = 0
orders_goods = {}
while ids_i < len(ids):
    cmd = ['cmd[0]=sale.order.get%3Fid%3D' + str(ids[ids_i])]
    for i in range(1, min(50, len(ids)-ids_i)):
        ids_i += 1
        cmd.append('cmd[' + str(i) + ']=sale.order.get%3Fid%3D' + str(ids[ids_i]))
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
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# удаление данных за вчера
        try:
            connection.execute(text("DELETE FROM " + config["BITRIX"]["TABLE_ORDERS_GOODS"] + " WHERE `orderId` IN (" + ','.join(ids) + ")"))
            connection.commit()
        except Exception as E:
            print (E)
            connection.rollback()
# добавление данных за вчера
        try:
            data.to_sql(name=config["BITRIX"]["TABLE_ORDERS_GOODS"], con=engine, if_exists='append', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
# удаление данных за вчера
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
            params={"database": config["DB"]["DB"], "query": "DELETE FROM " + config["DB"]["DB"] + "." + config["BITRIX"]["TABLE_ORDERS_GOODS"] + " WHERE `orderId` IN (" + ','.join(ids) + ")"}, headers={'Content-Type':'application/octet-stream'}, verify=False)
# добавление данных за вчера
        csv_file = data.to_csv().encode('utf-8')
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["BITRIX"]["TABLE_ORDERS_GOODS"] + ' FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (str(len(ids)) + ": " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
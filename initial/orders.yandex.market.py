# Скрипт для первоначального получения заказов Яндекс.Маркета
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * YANDEX_MARKET.ACCESS_TOKEN - Access Token (бессрочный, агентский) как альтернатива клиентскому набору Client Secret/Client Id/Refresh Token
# * YANDEX_MARKET.PERIODS - количество месяцев (всех выгрузок), будут выгружены данные за 30*PERIODS дней
# * YANDEX_MARKET.TABLE_ORDERS - имя результирующей таблицы для заказов

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

for i, TOKEN in enumerate(config["YANDEX_MARKET"]["ACCESS_TOKEN"].split(",")):
# получаем список магазинов (кампаний)
    r = requests.get("https://api.partner.market.yandex.ru/campaigns.json", params={
        'limit': 50
    }, headers = {'Authorization': 'Bearer ' + TOKEN}).json()
    for campaign in r['campaigns']:
# выгружаем данные по заказам периодам по 30 дней
        for period in range(PERIODS, 0, -1):
            date_since = (date.today() - timedelta(days=period*30).strftime('%d-%m-%Y')
            date_until = (date.today() - timedelta(days=(period-1)*30-1).strftime('%d-%m-%Y')
            r_orders = requests.get("https://api.partner.market.yandex.ru/campaigns/" + str(campaign['id']) + "/orders ", params={
                'fromDate': date_since,
                'toDate': date_until,
                'limit': 50
            }, headers = {'Authorization': 'Bearer ' + TOKEN}).json()
# формируем первичный список данных
            orders = []
            for order in r_orders['orders']:
                order_sum = 0
                for item in order['items']:
                    order_sum += float(item['price']) * int(item['count'])
                order['orderSum'] = order_sum
                order['campaign'] = campaign['id']
                orders.append(order)
# перебираем все заказы постранично
            while 'paging' in r_orders:
                r_orders_paging = requests.get("https://api.partner.market.yandex.ru/campaigns/" + str(campaign['id']) + "/orders ", params={
                    'fromDate': date_since,
                    'toDate': date_until,
                    'limit': 50,
                    'page_token': r_orders['paging']['nextPageToken']
                }, headers = {'Authorization': 'Bearer ' + TOKEN}).json()
                for order in r_orders['orders']:
                    order_sum = 0
                    for item in order['items']:
                        order_sum += float(item['price']) * int(item['count'])
                    order['orderSum'] = order_sum
                    order['campaign'] = campaign['id']
                    orders.append(order)
# формируем датафрейм из ответа API
            data = pd.DataFrame(orders)
# базовый процесс очистки: приведение к нужным типам
            for col in data.columns:
# приведение целых чисел
                if col in ["id", "itemsTotal", "deliveryTotal", "buyerItemsTotal", "buyerTotal", "buyerItemsTotalBeforeDiscount", "buyerTotalBeforeDiscount"]:
                    data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
                elif col in ["orderSum"]:
                    data[col] = data[col].fillna(0.0).astype(float)
# приведение дат
                elif col in ["creationDate", "updatedAt", "expiryDate"]:
                    data[col] = pd.to_datetime(data[col].fillna("2000-01-01"))
# приведение строк
                else:
                    data[col] = data[col].fillna('')
            if len(data):
# создаем таблицу в первый раз
                if table_not_created:
                    if config["DB"]["TYPE"] == "CLICKHOUSE":
                        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                            params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["YANDEX_MARKET"]["TABLE_ORDERS"]) + "  ENGINE=MergeTree ORDER BY (`id`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
                    table_not_created = False
                if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
                    try:
                        data.to_sql(name=config["YANDEX_MARKET"]["TABLE_ORDERS"], con=engine, if_exists='append', chunksize=100)
                    except Exception as E:
                        print (E)
                        connection.rollback()
                elif config["DB"]["TYPE"] == "CLICKHOUSE":
                    csv_file = data.to_csv().encode('utf-8')
                    requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                        params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_MARKET"]["TABLE_ORDERS"] + ' FORMAT CSV'},
                        headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
            print (campaign + " | " + date_since + "=>" + date_until + ": " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["YANDEX_MARKET"]["TABLE_ORDERS"] + " ADD INDEX creationdateidx (`creationDate`)"))
    connection.execute(text("ALTER TABLE " + config["YANDEX_MARKET"]["TABLE_ORDERS"] + " ADD INDEX updatedatidx (`updatedAt`)"))
    connection.commit()
    connection.close()
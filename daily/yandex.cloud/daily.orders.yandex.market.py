# Скрипт для ежедневного обновления заказов Яндекс.Маркета (за вчера)
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * YANDEX_MARKET_ACCESS_TOKEN - Access Token для приложения, имеющего доступ к статистике нужного сайта. Через запятую, если несколько
# * YANDEX_MARKET_TABLE_ORDERS - имя результирующей таблицы для заказов

# requirements.txt:
# pandas
# numpy
# requests
# datetime
# sqlalchemy

# timeout: 300
# memory: 512

import pandas as pd
import numpy as np
import os
import io
import requests
from datetime import datetime as dt
from datetime import date, timedelta
from sqlalchemy import create_engine, text

def handler(event, context):
    auth = {
        'X-ClickHouse-User': os.getenv('DB_USER'),
        'X-ClickHouse-Key': context.token["access_token"]
    }
    auth_post = auth.copy()
    auth_post['Content-Type'] = 'application/octet-stream'
    cacert = '/etc/ssl/certs/ca-certificates.crt'
    yesterday = (date.today() - timedelta(days=1)).strftime('%d-%m-%Y')

# подключение к БД
    if os.getenv('DB_TYPE') == "MYSQL":
        engine = create_engine('mysql+mysqldb://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB') + '?charset=utf8')
    elif os.getenv('DB_TYPE') == "POSTGRESQL":
        engine = create_engine('postgresql+psycopg2://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB') + '?client_encoding=utf8')
    elif os.getenv('DB_TYPE') == "MARIADB":
        engine = create_engine('mariadb+mysqldb://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB') + '?charset=utf8')
    elif os.getenv('DB_TYPE') == "ORACLE":
        engine = create_engine('oracle+pyodbc://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB'))
    elif os.getenv('DB_TYPE') == "SQLITE":
        engine = create_engine('sqlite:///' + os.getenv('DB_DB'))

# создание подключения к БД
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection = engine.connect()
        if os.getenv('DB_TYPE') in ["MYSQL", "MARIADB"]:
            connection.execute(text('SET NAMES utf8mb4'))
            connection.execute(text('SET CHARACTER SET utf8mb4'))
            connection.execute(text('SET character_set_connection=utf8mb4'))

    orders_total = 0
# нужно ли удалить все данные за вчера
    data_not_cleaned = True
# перебираем токены Яндекс.Маркета
    for i_credentials, TOKEN in enumerate(os.getenv('YANDEX_MARKET_ACCESS_TOKEN').split(",")):
# получаем список магазинов (кампаний)
        r = requests.get("https://api.partner.market.yandex.ru/campaigns.json", params={
            'limit': 50
        }, headers = {'Authorization': 'Bearer ' + TOKEN}).json()
        for campaign in r['campaigns']:
            r_orders = requests.get("https://api.partner.market.yandex.ru/campaigns/" + str(campaign['id']) + "/orders ", params={
                'updatedAtFrom': yesterday,
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
                    'updatedAtFrom': yesterday,
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
            orders_ids = ','.join(list(data["id"].values))
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
# обновление данных о расходах
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                if data_not_cleaned:
                    try:
                        connection.execute(text("DELETE FROM " + os.getenv('YANDEX_MARKET_TABLE_ORDERS') + " WHERE id IN (" + orders_ids + ")"))
                        connection.commit()
                    except Exception as E:
                        print (E)
                        connection.rollback()
                try:
                    data.to_sql(name=os.getenv('YANDEX_MARKET_TABLE_ORDERS'), con=engine, if_exists='append', chunksize=100)
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                if data_not_cleaned:
# удаляем данные за вчера
                    requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                        params={"database": os.getenv('DB_DB'), "query": "DELETE FROM " + os.getenv('DB_PREFIX') + "." + os.getenv('YANDEX_MARKET_TABLE_ORDERS') + " WHERE id in (" + orders_ids + ")"})
# добавляем новые данные
                csv_file = data.to_csv(index=False).encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": 'INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('YANDEX_MARKET_TABLE_ORDERS') + ' FORMAT CSV'},
                    data=csv_file, stream=True)
        data_not_cleaned = False
        orders_total += len(data)
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            connection.close()

    return {
        'statusCode': 200,
        'body': "LoadedOrders: " + str(orders_total)
    }
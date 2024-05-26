# Скрипт для ежедневного обновления данных Битрикс24 (пользовательских полей crm.contact, crm.lead, crm.deal) для облачных функций Яндекс.Облака
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * BITRIX24_WEBHOOK - URL вебхука (интеграции) Битрикс24
# * BITRIX24_TABLE_CONTACTS - имя таблицы с контактами
# * BITRIX24_TABLE_CONTACTS_UF - имя таблицы с пользовательскими полями контактов
# * BITRIX24_TABLE_LEADS - имя таблицы с лидами
# * BITRIX24_TABLE_LEADS_UF - имя таблицы с пользовательскими полями лидов
# * BITRIX24_TABLE_DEALS - имя таблицы со сделками
# * BITRIX24_TABLE_DEALS_UF - имя таблицы с пользовательскими полями сделок

# requirements.txt:
# pandas
# numpy
# requests
# datetime

# timeout: 300
# memory: 512

import pandas as pd
import numpy as np
import os
import io
import requests
from datetime import datetime as dt
from datetime import date, timedelta
import time

def handler(event, context):
    auth = {
        'X-ClickHouse-User': os.getenv('DB_USER'),
        'X-ClickHouse-Key': context.token["access_token"]
    }
    auth_post = auth.copy()
    auth_post['Content-Type'] = 'application/octet-stream'
    cacert = '/etc/ssl/certs/ca-certificates.crt'
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')

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

# словарь таблиц для обновления
    tables = {"crm.lead": "TABLE_LEADS_UF", "crm.contact": "TABLE_CONTACTS_UF", "crm.deal": "TABLE_DEALS_UF"}
# возвращаемая статистика
    ret = []

    for dataset in list(tables.keys()):
        current_table = os.getenv('BITRIX24_' + tables[dataset])
        parent_table =  os.getenv('BITRIX24_' + tables[dataset].replace("_UF", ""))
# Получение данных для обновления
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            ids = list(pd.read_sql("SELECT ID FROM " + parent_table + " WHERE DATE_MODIFY>'" + yesterday + "'", connection)["ID"].values)
        elif os.getenv('DB_TYPE') == "CLICKHOUSE":
            ids = requests.get("https://" + os.getenv('DB_HOST') + ":8443/?database=" + os.getenv('DB_DB') + "&query=SELECT ID FROM " + os.getenv('DB_PREFIX') + "." + parent_table + " WHERE DATE_MODIFY>'" + yesterday + "'",
                headers=auth, verify=cacert).text.split("\n")
# количество ID
        items_last_id = len(ids)
# счетчик количества объектов
        last_item_id = 1
        items = {}
# запросы пакетами по 50 объектов до исчерпания количества для загрузки
        while last_item_id < items_last_id:
            cmd = []
            for i in range(min(50, items_last_id-last_item_id)):
                cmd.append('cmd[' + str(i) + ']=' + dataset + '.get%3FID%3D' + str(ids[last_item_id]))
                last_item_id += 1
            items_req = requests.get(os.getenv('BITRIX24_WEBHOOK') + 'batch.json?' + '&'.join(cmd)).json()
            if "result" in items_req:
# разбор объектов из пакетного запроса
                for item in items_req["result"]["result"]:
                    if isinstance(item, str):
                        item = items_req["result"]["result"][item]
                    if "PHONE" in item:
                        item['phone1'] = item["PHONE"][0]["VALUE"]
                        if len(item['PHONE']) > 1:
                            item['phone2'] = item["PHONE"][1]["VALUE"]
                        else:
                            item['phone2'] = ''
                        if len(item['PHONE']) > 2:
                            item['phone3'] = item["PHONE"][2]["VALUE"]
                        else:
                            item['phone3'] = ''
                        del item['PHONE']
                    else:
                        item['phone1'] = ''
                    if "EMAIL" in item:
                        item['email'] = item["EMAIL"][0]["VALUE"]
                        del item['EMAIL']
                    else:
                        item['email'] = ''
                    if "IM" in item:
                        item['im1'] = item["IM"][0]["VALUE"]
                        if len(item['IM']) > 1:
                            item['im2'] = item["IM"][1]["VALUE"]
                        else:
                            item['im2'] = ''
                        if len(item['IM']) > 2:
                            item['im3'] = item["IM"][2]["VALUE"]
                        else:
                            item['im3'] = ''
                        del item['IM']
                    else:
                        item['im1'] = ''
                    items[int(item['ID'])] = item
                time.sleep(1)
# формируем датафрейм
        data = pd.DataFrame.from_dict(items, orient='index')
        del items
# базовый процесс очистки: приведение к нужным типам
        for i,col in enumerate(data.columns):
# приведение целых чисел
            if col in ["ID", "ASSIGNED_BY_ID", "CREATED_BY_ID", "MODIFY_BY_ID", "LEAD_ID", "ADDRESS_LOC_ADDR_ID", "ADDRESS_COUNTRY_CODE", "REG_ADDRESS_COUNTRY_CODE", "REG_ADDRESS_LOC_ADDR_ID", "LAST_ACTIVITY_BY", "SORT", "CATEGORY_ID", "COMPANY_ID", "CONTACT_ID"] or data.dtypes[i] in ["bool", "int64", "int32"]:
                data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["REVENUE", "OPPORTUNITY"] or data.dtypes[i] in ["float32", "float64"]:
# приведение дат
                data[col] = data[col].fillna('').replace('', 0.0).astype(float)
            elif col in ["DATE_CREATE", "DATE_MODIFY", "LAST_ACTIVITY_TIME", "CREATED_DATE", "DATE_CLOSED", "MOVED_TIME"] or data.dtypes[i] == "datetime64":
                data[col] = pd.to_datetime(data[col].fillna('').replace('', '2000-01-01T00:00:00+03:00').apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%S%z').strftime("%Y-%m-%d %H:%M:%S").replace('202-','2024-')))
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
            if "DATE_CREATE" in data.columns:
                data['ts'] = pd.DatetimeIndex(data["DATE_CREATE"]).asi8
                index = 'ts'
            else:
                index = 'ID'
# удаление старых данных
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                connection.execute(text("DELETE FROM " + current_table + " WHERE ID IN (" + ",".join(ids) + ")"))
                connection.commit()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                requests.post("https://" + os.getenv('DB_HOST') + ":8443/?database=" + os.getenv('DB_DB') + "&query=DELETE FROM " + os.getenv('DB_PREFIX') + "." + current_table + " WHERE ID IN (" + ",".join(ids) + ")",
                    headers=auth_post, verify=cacert)
# добавление новых данных
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
                try:
                    data.to_sql(name=current_table, con=engine, if_exists='append', chunksize=100)
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                csv_file = data.to_csv(index=False).encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": 'INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + current_table + ' FORMAT CSV'},
                    data=csv_file, stream=True)
        ret.append(dataset + "=" + str(len(data)))
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection.close()

    return {
        'statusCode': 200,
        'body': "Modified: " + ', '.join(ret)
    }
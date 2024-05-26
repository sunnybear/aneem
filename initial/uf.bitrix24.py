# Скрипт для первоначального получения пользовательских полей (UF) в дополнение к основным объектам из Битрикс24
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * BITRIX24.METHOD - BATCH (для пакетной загрузки) или SINGLE (для одиночной загрузки)
# * BITRIX24.WEBHOOK - URL вебхука (интеграции) Битрикс24
# * BITRIX24.TABLE_LEADS_UF - имя результирующей таблицы для пользовательских полей crm.lead
# * BITRIX24.TABLE_CONTACTS_UF - имя результирующей таблицы для пользовательских полей crm.contact

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

# выделение телефона, email, мессенджера в плоскую структуру
def bitrix24_crm_uf_plain_contacts (item):
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
    return item

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

# словарь таблиц для обновления
tables = {"crm.contact": "TABLE_CONTACTS_UF", "crm.deal": "TABLE_DEALS_UF"}
# загружаем справочники и дополнительные таблицы
for dataset in list(tables.keys()):
# если в настройках задана таблица - загружаем данные
    if tables[dataset] in config["BITRIX24"]:
        current_table = tables[dataset]
        parent_table = tables[dataset].replace("_UF", "")
# создаем таблицу для данных при наличии каких-либо данных
        table_not_created = True
# получение всех ID объектов
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            ids = list(pd.read_sql("SELECT ID FROM " + config["DB"]["DB"] + "." + config["BITRIX24"][parent_table], connection)["ID"].values)
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            ids_req = requests.get('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'SELECT ID FROM ' + config["DB"]["DB"] + '.' + config["BITRIX24"][parent_table]},
                verify=False)
            ids = list(ids_req.text.split("\n"))
# количество ID
        items_last_id = len(ids)
# счетчик количества объектов
        last_item_id = 1
        items = {}
# запросы пакетами по 50 объектов до исчерпания количества для загрузки
        while last_item_id < items_last_id:
            if config["BITRIX24"]["METHOD"] == "BATCH":
                cmd = []
                for i in range(min(50, items_last_id-last_item_id)):
                    cmd.append('cmd[' + str(i) + ']=' + dataset + '.get%3FID%3D' + str(ids[last_item_id]))
                    last_item_id += 1
                items_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'batch.json?' + '&'.join(cmd)).json()
                if "result" in items_req:
# разбор объектов из пакетного запроса
                    for item in items_req["result"]["result"]:
                        if isinstance(item, str):
                            item = items_req["result"]["result"][item]
                        item = bitrix24_crm_uf_plain_contacts(item)
                        items[int(item['ID'])] = item
# задержка для избежания исчерпания лимита запросов
                time.sleep(1)
            elif config["BITRIX24"]["METHOD"] == "SINGLE":
                items_req = requests.get(config["BITRIX24"]["WEBHOOK"] + dataset + '.get.json?ID=' + str(ids[last_item_id])).json()
                if "result" in items_req:
# разбор объектов из обычного запроса
                    item = items_req["result"]
                    item = bitrix24_crm_uf_plain_contacts(item)
                    items[int(item['ID'])] = item
                last_item_id += 1
            print (dataset + ": " + str(len(items)) + "/" + str(items_last_id))
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
                data["ts"] = pd.DatetimeIndex(data["DATE_CREATE"]).asi8
                index = 'ts'
            else:
                index = 'ID'
# создаем таблицу в первый раз
            if table_not_created:
                if config["DB"]["TYPE"] == "CLICKHOUSE":
                    requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                        params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["BITRIX24"][current_table]) + "  ENGINE=MergeTree ORDER BY (`" + index + "`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
                table_not_created = False
            if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
                try:
                    data.to_sql(name=config["BITRIX24"][current_table], con=engine, if_exists='append', chunksize=100)
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif config["DB"]["TYPE"] == "CLICKHOUSE":
                csv_file = data.to_csv(index=False).encode('utf-8')
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                    params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["BITRIX24"][current_table] + ' FORMAT CSV'},
                    headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
        print (dataset + " UF = " + str(items_last_id))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
# Скрипт для первоначального получения таблиц справочников из CRM Битрикс24: crm.status, crm.dealcategory, crm.dealcategory.stage
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * BITRIX24.METHOD - BATCH (для пакетной загрузки) или SINGLE (для одиночной загрузки)
# * BITRIX24.WEBHOOK - URL вебхука (интеграции) Битрикс24
# * BITRIX24.TABLE_STATUSES - имя результирующей таблицы для crm.status
# * BITRIX24.TABLE_DEAL_CATEGORIES - имя результирующей таблицы для crm.dealcategory
# * BITRIX24.TABLE_DEAL_CATEGORY_STAGES - имя результирующей таблицы для crm.dealcategory.stage

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

# словарь таблиц для обновления
table = {"crm.status": "TABLE_STATUSES", "crm.dealcategory": "TABLE_DEAL_CATEGORIES", "crm.dealcategory.stage": "TABLE_DEAL_CATEGORY_STAGES"}

for dataset in list(table.keys()):
    current_table = table[dataset]
# создаем таблицу для данных при наличии каких-либо данных
    table_not_created = True
# получение количества элементов
    items = requests.get(config["BITRIX24"]["WEBHOOK"] + dataset + '.list.json?ORDER[ID]=ASC&FILDER[>ID]=0').json()
# общее количество элементов
    items_total = int(items["total"])
# текущий ID элемента - для следующего запроса
    last_item_id = 0
# счетчик количества элементов
    items_current = 0
# запросы пакетами по 50*50 элементов до исчерпания количества для загрузки
    while items_current < items_total:
        items = {}
        if config["BITRIX24"]["METHOD"] == "BATCH":
            cmd = ['cmd[0]=' + dataset + '.list%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D' + str(last_item_id)]
            for i in range(1, 50):
                cmd.append('cmd['+str(i)+']=' + dataset + '.list%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D%24result%5B'+str(i-1)+'%5D%5B49%5D%5BID%5D')
            items_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'batch.json?' + '&'.join(cmd)).json()
# разбор элементов из пакетного запроса
        for item_group in items_req["result"]["result"]:
            for item in item_group:
                last_item_id = int(item['ID'])
                items[last_item_id] = item
        elif config["BITRIX24"]["METHOD"] == "SINGLE":
            items_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'crm.item.list.json?ORDER[ID]=ASC&FILDER[>ID]=' + str(last_item_id)).json()
# разбор элементов из обычного запроса
            for item in items_req["result"]:
                last_item_id = int(item['ID'])
                items[last_item_id] = item
        items_current += len(items)
# формируем датафрейм
        data = pd.DataFrame.from_dict(items, orient='index')
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["ID", "ASSIGNED_BY_ID", "CREATED_BY_ID", "MODIFY_BY_ID", "LEAD_ID", "ADDRESS_LOC_ADDR_ID", "ADDRESS_COUNTRY_CODE", "REG_ADDRESS_COUNTRY_CODE", "REG_ADDRESS_LOC_ADDR_ID", "LAST_ACTIVITY_BY", "SORT", "CATEGORY_ID"]:
                data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["REVENUE"]:
# приведение дат
                data[col] = data[col].fillna('').replace('', 0.0).astype(float)
            elif col in ["DATE_CREATE", "DATE_MODIFY", "DATE_CLOSED", "MOVED_TIME", "LAST_ACTIVITY_TIME"]:
                data[col] = pd.to_datetime(data[col].fillna('').replace('', '2000-01-01T00:00:00+03:00').apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%S%z').strftime("%Y-%m-%d %H:%M:%S").replace('202-','2024-')))
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
            if "DATE_CREATE" in data.columns:
                data["ts"] = pd.DatetimeIndex(data["DATE_CREATE"]).asi8
                index = "ts"
            else:
                index = "ID"
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
                csv_file = data.to_csv().encode('utf-8')
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                    params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["BITRIX24"][current_table] + ' FORMAT CSV'},
                    headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
        print (dataset + "=" + str(last_item_id) + ": " + str(items_current))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
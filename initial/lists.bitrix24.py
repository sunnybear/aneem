# Скрипт для первоначального получения элементов списков из CRM Битрикс24: lists.element
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * BITRIX24.METHOD - BATCH (для пакетной загрузки) или SINGLE (для одиночной загрузки)
# * BITRIX24.WEBHOOK - URL вебхука (интеграции) Битрикс24
# * BITRIX24.IDS_LISTS - ID списков, которые нужно выгрузить, через запятую
# * BITRIX24.TABLE_LISTS - базовое имя таблиц для списков

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

for LIST_ID in config["BITRIX24"]["IDS_LISTS"].split(","):
    LIST_ID = LIST_ID.strip()

# создаем таблицу для данных при наличии каких-либо данных
    table_not_created = True
# получение количества элементов
    elements = requests.get(config["BITRIX24"]["WEBHOOK"] + 'lists.element.get.json?IBLOCK_ID=' + LIST_ID + '&IBLOCK_TYPE_ID=lists&ORDER[ID]=ASC&FILDER[>ID]=0').json()
# общее количество элементов
    elements_total = int(elements["total"])
# текущий ID элемента - для следующего запроса
    last_element_id = 0
# счетчик количества элементов
    elements_current = 0
# запросы пакетами по 50*50 элементов до исчерпания количества для загрузки
    while elements_current < elements_total:
        elements = {}
        if config["BITRIX24"]["METHOD"] == "BATCH":
            cmd = ['cmd[0]=lists.element.get%3FIBLOCK_ID%3D' + LIST_ID + '%26IBLOCK_TYPE_ID%3Dlists%26start%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D' + str(last_element_id)]
            for i in range(1, 50):
                cmd.append('cmd['+str(i)+']=lists.element.get%3FIBLOCK_ID%3D' + LIST_ID + '%26IBLOCK_TYPE_ID%3Dlists%26start%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D%24result%5B'+str(i-1)+'%5D%5B49%5D%5BID%5D')
            elements_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'batch.json?' + '&'.join(cmd)).json()
# разбор элементов из пакетного запроса
            for element_group in elements_req["result"]["result"]:
                for element in element_group:
                    last_element_id = int(element['ID'])
                    elements[last_element_id] = element
        elif config["BITRIX24"]["METHOD"] == "SINGLE":
            elements_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'lists.element.get.json?IBLOCK_ID=' + LIST_ID + '&IBLOCK_TYPE_ID=lists&ORDER[ID]=ASC&FILDER[>ID]=' + str(last_element_id)).json()
# разбор элементов из обычного запроса
            for element in elements_req["result"]:
                last_element_id = int(element['ID'])
                elements[last_element_id] = element
        elements_current += len(elements)
# формируем датафрейм
        data = pd.DataFrame.from_dict(elements, orient='index')
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["ID", "IBLOCK_ID", "CREATED_BY", "IBLOCK_SECTION_ID"]:
                data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
# создаем таблицу в первый раз
            if table_not_created:
                if config["DB"]["TYPE"] == "CLICKHOUSE":
                    requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                        params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["BITRIX24"]["TABLE_LISTS"] + LIST_ID) + "  ENGINE=MergeTree ORDER BY (`ID`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
                else:
                    connection.execute(text("DROP TABLE IF EXISTS " + config["BITRIX24"]["TABLE_LISTS"] + LIST_ID))
                    connection.commit()
                table_not_created = False
            if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
                try:
                    data.to_sql(name=config["BITRIX24"]["TABLE_LISTS"] + LIST_ID, con=engine, if_exists='replace', chunksize=100)
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif config["DB"]["TYPE"] == "CLICKHOUSE":
                csv_file = data.to_csv().encode('utf-8')
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                    params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["BITRIX24"]["TABLE_LISTS"] + LIST_ID + ' FORMAT CSV'},
                    headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
        print (str(LIST_ID) + ": " + str(len(data)) + "/" + str(elements_total))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
# Скрипт для ежедневного обновления данных по спискам Битрикс24 (lists) для облачных функций Яндекс.Облака
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * BITRIX24_WEBHOOK - URL вебхука (интеграции) Битрикс24
# * BITRIX24_IDS_LISTS - ID списков, которые нужно выгрузить, через запятую
# * BITRIX24_TABLE_LISTS - базовое имя таблиц для списков

# requirements.txt:
# pandas
# numpy
# requests

# timeout: 300
# memory: 512

import pandas as pd
import numpy as np
import os
import io
import requests

def handler(event, context):
    auth = {
        'X-ClickHouse-User': os.getenv('DB_USER'),
        'X-ClickHouse-Key': context.token["access_token"]
    }
    auth_post = auth.copy()
    auth_post['Content-Type'] = 'application/octet-stream'
    cacert = '/etc/ssl/certs/ca-certificates.crt'

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

    r = []
# перебираем все ID списков
    for LIST_ID in os.getenv('BITRIX24_IDS_LISTS').split(","):
        LIST_ID = LIST_ID.strip()
# формируем запрос для получения общего числа элементов
        try:
            elements_req = requests.get(os.getenv('BITRIX24_WEBHOOK') + 'lists.element.get.json?IBLOCK_ID=' + LIST_ID + '&IBLOCK_TYPE_ID=lists&ORDER[ID]=ASC&FILDER[>ID]=0').json()
# делаем еще одну попытку
        except Exception as E:
            elements_req = requests.get(os.getenv('BITRIX24_WEBHOOK') + 'lists.element.get.json?IBLOCK_ID=' + LIST_ID + '&IBLOCK_TYPE_ID=lists&ORDER[ID]=ASC&FILDER[>ID]=0').json()
        elements = {}
# общее количество элементов
        elements_total = int(elements_req["total"])
# текущий ID элемента - для следующего запроса
        last_element_id = 0
# счетчик количества элементов
        elements_current = 0
# запросы пакетами по 50*50 элементов до исчерпания количества для загрузки
        while elements_current < elements_total:
            cmd = ['cmd[0]=lists.element.get%3FIBLOCK_ID%3D' + LIST_ID + '%26IBLOCK_TYPE_ID%3Dlists%26start%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D' + str(last_element_id)]
            for i in range(1, 20):
                cmd.append('cmd['+str(i)+']=lists.element.get%3FIBLOCK_ID%3D' + LIST_ID + '%26IBLOCK_TYPE_ID%3Dlists%26start%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D%24result%5B'+str(i-1)+'%5D%5B49%5D%5BID%5D')
            try:
                elements_req = requests.get(os.getenv('BITRIX24_WEBHOOK') + 'batch.json?' + '&'.join(cmd)).json()
# делаем еще одну попытку
            except Exception as E:
                elements_req = requests.get(os.getenv('BITRIX24_WEBHOOK') + 'batch.json?' + '&'.join(cmd)).json()
# разбор элементов из пакетного запроса
            for element_group in elements_req["result"]["result"]:
                for element in element_group:
                    last_element_id = int(element['ID'])
                    elements[last_element_id] = element
            elements_current = len(elements)

# преобразуем словарь в датафрейм
        data = pd.DataFrame.from_dict(elements, orient='index')
        del elements
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["ID", "IBLOCK_ID", "CREATED_BY", "IBLOCK_SECTION_ID"]:
                data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
# замена данных
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при замене данных
                try:
                    data.to_sql(name=os.getenv('BITRIX24_TABLE_LISTS') + LIST_ID, con=engine, if_exists='replace', chunksize=100)
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
# пересоздаем таблицу для clickhouse
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": (pd.io.sql.get_schema(data, os.getenv('BITRIX24_TABLE_LISTS') + LIST_ID) + "  ENGINE=MergeTree ORDER BY (`ID`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + os.getenv('DB_PREFIX') + ".").replace("INTEGER", "Int64")})
                csv_file = data.to_csv().encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": 'INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('BITRIX24_TABLE_LISTS') + LIST_ID + ' FORMAT CSV'},
                    data=csv_file, stream=True)
        r.append(LIST_ID + ' => ' + str(len(data)))
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection.close()

    return {
        'statusCode': 200,
        'body': "Modified: " + ', '.join(r)
    }
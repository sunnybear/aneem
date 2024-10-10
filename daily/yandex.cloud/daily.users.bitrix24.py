# Скрипт для ежедневного полного обновления данных по пользователям Битрикс24 (user) для облачных функций Яндекс.Облака
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * BITRIX24_WEBHOOK - URL вебхука (интеграции) Битрикс24
# * BITRIX24_METHOD - BATCH (пакетные) или SINGLE (одиночные) запросы
# * BITRIX24_TABLE_USERS - имя результирующей таблицы для user

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

# получение количества пользователей
    users = requests.get(os.getenv('BITRIX24_WEBHOOK') + 'user.get.json?ORDER[ID]=ASC&FILDER[>ID]=0').json()
# общее количество пользователей
    users_total = int(users["total"])
# текущий ID контакта - для следующего запроса
    last_user_id = 0
# счетчик количества контактов
    users_current = 0
# запросы пакетами по 50*50 контактов до исчерпания количества для загрузки
    while users_current < users_total:
        users = {}
        if os.getenv('BITRIX24_METHOD') == "BATCH":
            cmd = ['cmd[0]=user.get%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D' + str(last_user_id)]
            for i in range(1, 50):
                cmd.append('cmd['+str(i)+']=user.get%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D%24result%5B'+str(i-1)+'%5D%5B49%5D%5BID%5D')
            users_req = requests.get(os.getenv('BITRIX24_WEBHOOK') + 'batch.json?' + '&'.join(cmd)).json()
# разбор пользователей из пакетного запроса
            for user_group in users_req["result"]["result"]:
                for user in user_group:
                    last_user_id = int(user['ID'])
                    users[last_user_id] = user
        elif os.getenv('BITRIX24_METHOD') == "SINGLE":
            users_req = requests.get(os.getenv('BITRIX24_WEBHOOK') + 'user.get.json?ORDER[ID]=ASC&FILDER[>ID]=' + str(last_user_id)).json()
# разбор пользователей из обычного запроса
            for user in users_req["result"]:
                last_user_id = int(user['ID'])
                users[last_user_id] = user
        users_current += len(users)
# формируем датафрейм
        data = pd.DataFrame.from_dict(users, orient='index')
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["ID", "XML_ID", "TIME_ZONE_OFFSET", "ACTIVE"]:
                data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение дат
            elif col in ["LAST_LOGIN", "DATE_REGISTER"]:
                data[col] = pd.to_datetime(data[col].fillna('').replace('', '2000-01-01T00:00:00+03:00').apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%S%z').strftime("%Y-%m-%d %H:%M:%S").replace('202-','2024-')))
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
# удаление старых данных
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                connection.execute(text("DELETE FROM " + os.getenv('BITRIX24_TABLE_USERS') + " WHERE ID>0"))
                connection.commit()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                requests.post("https://" + os.getenv('DB_HOST') + ":8443/?database=" + os.getenv('DB_DB') + "&query=DELETE FROM " + os.getenv('DB_PREFIX') + "." + os.getenv('BITRIX24_TABLE_USERS') + " WHERE ID>0",
                    headers=auth_post, verify=cacert)
# добавление новых данных
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
                try:
                    data.to_sql(name=os.getenv('BITRIX24_TABLE_USERS'), con=engine, if_exists='append', chunksize=100)
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                csv_file = data.to_csv(index=False).encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": 'INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('BITRIX24_TABLE_USERS') + ' FORMAT CSV'},
                    data=csv_file, stream=True)
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            connection.close()

    return {
        'statusCode': 200,
        'body': "Modified: " + str(len(data))
    }
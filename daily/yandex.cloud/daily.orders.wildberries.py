# Скрипт для ежедневного обновления статистики по продажам (заказам) из кабинета Wildberries
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * WILDBERRIES_ACCESS_TOKEN - Access Token, имеющий доступ к статистике нужного кабинета (или несколько - через запятую)
# * WILDBERRIES_TABLE_ORDERS - имя результирующей таблицы для заказов

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
from tapi_yandex_direct import YandexDirect
import datetime as dt
from sqlalchemy import create_engine, text

def handler(event, context):
    auth = {
        'X-ClickHouse-User': os.getenv('DB_USER'),
        'X-ClickHouse-Key': context.token["access_token"]
    }
    auth_post = auth.copy()
    auth_post['Content-Type'] = 'application/octet-stream'
    cacert = '/etc/ssl/certs/ca-certificates.crt'
    date_yesterday = (dt.date.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d')

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

    ret = []
# перебираем все доступы к рекламным кабинетам
    for i_credentials, TOKEN in enumerate(os.getenv('WILDBERRIES_ACCESS_TOKEN').split(",")):
# нужно ли удалить все данные за вчера
        data_not_cleaned = True
        TOKEN = TOKEN.strip()
# отправка запроса
        result = requests.get('https://marketplace-api.wildberries.ru/api/v3/orders',
            headers = {'Authorization': TOKEN},
            params = {'dateFrom' : date_yesterday, 'flag' : 0})
		
# формируем датафрейм из ответа API
        data = pd.DataFrame(result.json())
		data["account"] = hash(TOKEN)
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["barcode"]:
                data[col] = data[col].fillna(0).replace('--', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["totalPrice", "discountPercent", "spp", "finishedPrice", "priceWithDisc"]:
                data[col] = data[col].fillna(0.0).replace('--', 0.0).astype(float)
# приведение дат
            elif col in ["date", "lastChangeDate", "cancelDate"]:
                data[col] = pd.to_datetime(data[col])
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
            data["ts"] = pd.DatetimeIndex(data["date"]).asi8
            if data_not_cleaned:
# удаление старых данных
                if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                    try:
                        connection.execute(text("DELETE FROM " + os.getenv('WILDBERRIES_TABLE_ORDERS') + " WHERE `date`>='" + date_yesterday + "' AND account='" + hash(TOKEN) + "'"))
                        connection.commit()
                    except Exception as E:
                        print (E)
                        connection.rollback()
                elif os.getenv('DB_TYPE') == "CLICKHOUSE":
# удаление старых данных
                    requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth, verify=cacert,
                        params={"database": os.getenv('DB_DB'), "query": "DELETE FROM " + os.getenv('DB_PREFIX') + "." + os.getenv('WILDBERRIES_TABLE_ORDERS') + " WHERE `date`>='" + date_yesterday + "' AND account='" + hash(token) + "'"})
            data_not_cleaned = False
# добавление новых данных
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                try:
                    data.to_sql(name=os.getenv('WILDBERRIES_TABLE_ORDERS'), con=engine, if_exists='append', chunksize=100)
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                csv_file = data.to_csv().encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443/?database=' + os.getenv('DB_DB') + '&query=INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('WILDBERRIES_TABLE_ORDERS') + ' FORMAT CSV',
                    headers=auth_post, data=csv_file, stream=True)
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            connection.close()
        ret.append(str(credentials_i) + ' => ' + str(len(data)))

    return {
        'statusCode': 200,
        'body': "LoadedCosts: " + ', '.join(ret)
    }
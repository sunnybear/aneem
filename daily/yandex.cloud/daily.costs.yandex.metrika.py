# Скрипт для ежедневного обновления расходов из Яндекс.Метрики (за вчера)
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * YANDEX_METRIKA_ACCESS_TOKEN - Access Token для приложения, имеющего доступ к статистике нужного сайта. Через запятую, если несколько
# * YANDEX_METRIKA_COUNTER_ID - ID сайта, статистику которого нужно выгрузить. Через запятую, если несколько
# * YANDEX_METRIKA_TABLE_COSTS - имя результирующей таблицы для расходов

# requirements.txt:
# pandas
# numpy
# requests
# datetime
# tapi_yandex_metrika
# sqlalchemy

# timeout: 300
# memory: 512

import pandas as pd
import numpy as np
import os
import io
import requests
from tapi_yandex_metrika import YandexMetrikaStats
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
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday_1 = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')

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

    costs_total = 0
# нужно ли удалить все данные за вчера
    data_not_cleaned = True
# перебираем токены Яндекс.Метрики
    for i_credentials, TOKEN in enumerate(os.getenv('YANDEX_METRIKA_ACCESS_TOKEN').split(",")):
        api = YandexMetrikaStats(access_token=TOKEN.strip())
        COUNTER_ID = os.getenv('YANDEX_METRIKA_COUNTER_ID').split(",")[i_credentials].strip()
# Создание запроса на выгрузку данных (30 дней назад)
        params = {
            "ids": COUNTER_ID,
            "metrics": "ym:ev:expensesRUB,ym:ev:visits,ym:ev:expenseClicks",
            "dimensions": "ym:ev:date,ym:ev:lastExpenseSource,ym:ev:lastExpenseMedium,ym:ev:lastExpenseCampaign",
            "date1": yesterday_1,
            "date2": yesterday,
            "limit": 100000
        }
# отправляем запрос API
        result = api.stats().get(params=params)
# формируем датафрейм по отдельности из каждой части
        data = pd.DataFrame(result().to_values(), columns=result.columns)
        data["ym:ev:counterId"] = COUNTER_ID
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["ym:ev:visits", "ym:ev:expenseClicks"]:
                data[col] = data[col].fillna('').replace('', 0).replace('None', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["ym:ev:expensesRUB"]:
                data[col] = data[col].fillna(0.0).astype(float)
# приведение дат
            elif col in ["ym:ev:date"]:
                data[col] = pd.to_datetime(data[col].fillna("2000-01-01").apply(lambda x: dt.strptime(x, "%Y-%m-%d")))
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
# добавляем метку времени
            data["ts"] = pd.DatetimeIndex(data["ym:ev:date"]).asi8
# обновление данных о расходах
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                if data_not_cleaned:
                    try:
                        connection.execute(text("DELETE FROM " + os.getenv('YANDEX_METRIKA_TABLE_COSTS') + " WHERE `ym:ev:date`>='" + yesterday_1 + "'"))
                        connection.commit()
                    except Exception as E:
                        print (E)
                        connection.rollback()
                try:
                    data.to_sql(name=os.getenv('YANDEX_METRIKA_TABLE_COSTS'), con=engine, if_exists='append', chunksize=100)
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                if data_not_cleaned:
# удаляем данные за вчера
                    requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                        params={"database": os.getenv('DB_DB'), "query": "DELETE FROM " + os.getenv('DB_PREFIX') + "." + os.getenv('YANDEX_METRIKA_TABLE_COSTS') + " WHERE `ym:ev:date`>='" + yesterday_1 + "'"})
# добавляем новые данные
                csv_file = data.to_csv(index=False).encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": 'INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('YANDEX_METRIKA_TABLE_COSTS') + ' FORMAT CSV'},
                    data=csv_file, stream=True)
        data_not_cleaned = False
        costs_total += len(data)
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            connection.close()

    return {
        'statusCode': 200,
        'body': "LoadedCosts: " + str(costs_total)
    }
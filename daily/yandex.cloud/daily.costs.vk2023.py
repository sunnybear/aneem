# Скрипт для ежедневного обновления статистики по кампаниям (включая расходы) из кабинета вКонтакте после 2023 года в Яндекс.Облаке
# Необходимо в settings.ini указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * VK2023_ACCESS_TOKEN - Access Token (бессрочный, агентский) как альтернатива клиентскому набору Client Secret/Client Id/Refresh Token
# * VK2023_CLIENT_SECRET - Client Secret из настроек аккаунта
# * VK2023_CLIENT_ID - Client Id из настроек аккаунта
# * VK2023_REFRESH_TOKEN - Refresh Token (получается после запроса ACCESS TOKEN в API ВК), используется для обновления клиентского ACCESS TOKEN
# * VK2023_TABLE - имя результирующей таблицы для статистики (расходов)

# requirements.txt:
# pandas
# numpy
# requests
# datetime
# sqlalchemy

# timeout: 300
# memory: 256

# импорт общих библиотек
import pandas as pd
import numpy as np
import os
import io
import requests
from datetime import datetime as dt
from datetime import date, timedelta
import time
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
    yesterday_1 = (date.today() - timedelta(days=2)).strftime('%Y-%m-%d')

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

# метка удаления данных за вчера
    data_cleanup = True
    ret = []

# перебор всех токенов при необходимости
    if os.getenv('VK_2023_ACCESS_TOKEN'):
        TOKENS = os.getenv('VK_2023_ACCESS_TOKEN').split(",")
    else:
        TOKENS = os.getenv('VK_2023_REFRESH_TOKEN').split(",")

    for i, TOKEN in enumerate(TOKENS):
# обновляем токен ВК, если требуется
        if os.getenv('VK_2023_ACCESS_TOKEN'):
            vk2023_access_token = TOKEN
        else:
            r_refresh = requests.post('https://ads.vk.com/api/v2/oauth2/token.json', data={
                'grant_type': 'refresh_token',
                'refresh_token': TOKEN,
                'client_id': os.getenv('VK_2023_CLIENT_ID').split(",")[i],
                'client_secret': os.getenv('VK_2023_CLIENT_SECRET').split(",")[i]
            }).json()
            vk2023_access_token = r_refresh['access_token']

# Задержка в 1 секунду для избежания превышения лимитов по запросам
        time.sleep(1)
# Создание запроса на выгрузку данных (помесячно)
        r = requests.get("https://ads.vk.com/api/v2/statistics/ad_plans/day.json", params={
            'date_from': date_since,
            'date_to': date_until,
            'metrics': 'base'
        }, headers = {'Authorization': 'Bearer ' + vk2023_access_token}).json()
# формируем первичный список данных
        items = []
        if "items" in r.keys():
            for k in r["items"]:
                for row in k["rows"]:
                    item = row["base"]
                    item["campaign_id"] = k["id"]
                    if "vk" in item.keys():
                        item["vk_goals"] = item["vk"]["goals"]
                        item["vk_cpa"] = item["vk"]["cpa"]
                        item["vk_cr"] = item["vk"]["cr"]
                        del item["vk"]
                    else:
                        item["vk_goals"] = 0
                        item["vk_cpa"] = 0.0
                        item["vk_cr"] = 0.0
                    item["date"] = row["date"]
                    items.append(item)
# формируем датафрейм из ответа API
        data = pd.DataFrame(items)
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["shows", "clicks", "goals", "vk_goals", "campaign_id"]:
                data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["spent", "cpm", "cpc", "cpa", "ctr", "cr", "vk_cpa", "vk_cr"]:
                data[col] = data[col].fillna(0.0).astype(float)
# приведение дат
            elif col in ["date"]:
                data[col] = pd.to_datetime(data[col].fillna("2000-01-01"))
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
# добавляем метку времени
            data["ts"] = pd.DatetimeIndex(data["date"]).asi8
# удаляем данные за вчера
            if data_cleanup:
                if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                    try:
                        connection.execute(text("DELETE FROM " + os.getenv('VK2023_TABLE') + " WHERE `date`>='" + yesterday_1 + "'"))
                        connection.commit()
                    except Exception as E:
                        print (E)
                        connection.rollback()
                elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                    requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                        params={"database": os.getenv('DB_DB'), "query": "DELETE FROM " + os.getenv('DB_PREFIX') + "." + os.getenv('VK2023_TABLE') + " WHERE `date`>='" + yesterday_1 + "'"})
                data_cleanup = False
# добавление данных за вчера
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
                try:
                    data.to_sql(name=os.getenv('VK2023_TABLE'), con=engine, if_exists='append', chunksize=100)
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                csv_file = data.to_csv().encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": 'INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('VK2023_TABLE') + ' FORMAT CSV'},
                    data=csv_file, stream=True)
        ret.append("TOKEN" + str(i) + " | " + date_since + "=>" + date_until + ": " + str(len(data)))

    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection.close()

    return {
        'statusCode': 200,
        'body': "LoadedCosts: " + '\n'.join(ret)
    }
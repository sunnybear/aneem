# Ежедневный скрипт для получения +- полного списка кампаний и UTM меток объявлений кабинетов вКонтакте после 2023 года в Яндекс.Облаке
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
# * VK2023_TABLE_UTMS - имя результирующей таблицы для UTM меток

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

# создаем таблицу для данных при наличии каких-либо данных
    table_not_created = True
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
# Создание запроса на выгрузку пакетов групп объявлений
        r_packages = requests.get("https://ads.vk.com/api/v2/packages.json", headers = {'Authorization': 'Bearer ' + vk2023_access_token}).json()
# формируем список пакетов с UTM
        packages = {}
        if "items" in r_packages.keys():
            for k in r_packages["items"]:
                if k["utm"]:
# преобразуем строку адреса из URL Decoded формата
                    href = requests.utils.unquote(k["utm"]).replace("+", " ")
                    utm_values = []
                    for utm in ['utm_source', 'utm_medium', 'utm_campaign']:
                        if href.find(utm) > -1:
                            utm_start = href.find(utm)
                            utm_end = href[href.find(utm):].find('&')
                            if utm_end == -1:
                                utm_end = len(href)
                            else:
                                utm_end += utm_start
                            utm_values.append(href[utm_start + len(utm) + 1:utm_end])
                        else:
                            utm_values.append('')
                    packages[k["id"]] = [href, utm_values[0], utm_values[1], utm_values[2]]
                else:
                    packages[k["id"]] = ['', 'cpc', 'vk', '']
# Создание запроса на выгрузку объявлений
        r_ads = requests.get("https://ads.vk.com/api/v2/ad_plans.json?fields=id,package_id,name", headers = {'Authorization': 'Bearer ' + vk2023_access_token}).json()
# формируем список обхявлений с UTM из пакетов
        items = []
        if "items" in r_ads.keys():
            for k in r_ads["items"]:
                if k["package_id"] in packages.keys():
                    if packages[k["package_id"]][3] == '':
                        utm_campaign = k["id"]
                    else:
                        utm_campaign = packages[k["package_id"]][3].replace('{{campaign_id}}', str(k["id"])).replace('{{campaign_name}}', k["name"])
                    items.append([k["id"], k["name"], packages[k["package_id"]][0], packages[k["package_id"]][1], packages[k["package_id"]][2], utm_campaign])
# формируем датафрейм из полученных меток
        data = pd.DataFrame(items, columns=["CampaignId", "CampaignName", "CampaignHref", "UTMSource", "UTMMedium", "UTMCampaign"])
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["CampaignId"]:
                data[col] = data[col].fillna(0).astype(np.int64)
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
# создаем таблицу в первый раз
            if table_not_created:
                if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                    connection.execute(text("DROP TABLE IF EXISTS " + os.getenv('VK2023_TABLE_UTMS')))
                    connection.commit()
                elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                    requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                        params={"database": os.getenv('DB_DB'), "query": (pd.io.sql.get_schema(data.reset_index(), os.getenv('VK2023_TABLE_UTMS')) + "  ENGINE=MergeTree ORDER BY (`index`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + os.getenv('DB_PREFIX') + ".").replace("INTEGER", "Int64")})
                table_not_created = False
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
                try:
                    data.to_sql(name=os.getenv('VK2023_TABLE_UTMS'), con=engine, if_exists='append', chunksize=100)
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                csv_file = data.reset_index().to_csv().encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": 'INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('VK2023_TABLE_UTMS') + ' FORMAT CSV'},
                    data=csv_file, stream=True)
        ret.append("TOKEN" + str(i) + " | " + str(len(data)))

    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection.close()

    return {
        'statusCode': 200,
        'body': "LoadedUTMs: " + '\n'.join(ret)
    }
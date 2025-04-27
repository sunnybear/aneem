# Скрипт для получения полного списка кампаний и UTM меток объявлений кабинета Яндекс.Директа для облачных функций Яндекс.Облака
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * YANDEX_DIRECT_ACCESS_TOKEN - Access Token для приложения, имеющего доступ к статистике кампаний
# * YANDEX_DIRECT_ACCESS_LOGIN - Email, имеющий доступ к статистике кампаний
# * YANDEX_DIRECT_TABLE - имя результирующей таблицы для расходов Яндекс.Директ
# * YANDEX_DIRECT_TABLE_UTMS - имя результирующей таблицы для UTM меток

# requirements.txt:
# pandas
# numpy
# requests
# datetime
# tapi_yandex_direct
# sqlalchemy

# timeout: 300
# memory: 256

import pandas as pd
import numpy as np
import os
import io
import requests
from tapi_yandex_direct import YandexDirect
import datetime as dt
from sqlalchemy import create_engine, text
from io import StringIO

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

# перебираем все доступы к рекламным кабинетам
    for i_credentials, TOKEN in enumerate(os.getenv('YANDEX_DIRECT_ACCESS_TOKEN').split(",")):
        TOKEN = TOKEN.strip()
        LOGIN = os.getenv('YANDEX_DIRECT_ACCESS_LOGIN').split(",")[i_credentials].strip()
# создание подключения к API Яндекс.Директ
        client = YandexDirect(
            access_token = TOKEN,
            login = LOGIN,
            is_sandbox = False,
            retry_if_not_enough_units = False,
            language = "ru",
            retry_if_exceeded_limit = True,
            retries_if_server_error = 5,
            processing_mode = "offline",
            wait_report = True,
            return_money_in_micros = False,
            skip_report_header = True,
            skip_column_header = False,
            skip_report_summary = True
        )

        campaigns = []
# Основной способ: получаем CampaignId из текущих расходов
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            campaigns = pd.read_sql("SELECT distinct CampaignId, CampaignName FROM " + os.getenv('YANDEX_DIRECT_TABLE'), connection)
        elif os.getenv('DB_TYPE') == "CLICKHOUSE":
            campaigns = pd.read_csv(StringIO(requests.get('https://' + os.getenv('DB_HOST') + ':8443/?database=' + os.getenv('DB_DB') + '&query=SELECT distinct CampaignId, CampaignName FROM ' + os.getenv('YANDEX_DIRECT_TABLE'),
                verify=cacert, headers=auth).text), delimiter="\t")
        campaigns.columns = ["Id", "Name"]

# Дополнительный способ: получаем все кампании из аккаунта
        body = {
            "method": "get",
            "params": {"SelectionCriteria": {}, "FieldNames": ["Id", "Name"]}
        }
        campaigns_yd = client.campaigns().post(data=body)
# удаляем дубли
        campaigns = pd.concat([campaigns, pd.DataFrame(campaigns_yd().items())]).drop_duplicates('Id').reset_index()

# исходный список кампаний с UTM метками
        items = []
# получаем все объявления для данной кампании, интересует только ссылка в объявлении
        for cid in campaigns["Id"].values:
            cname = campaigns.loc[campaigns["Id"]==cid]["Name"].values[0]
            body = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {"CampaignIds": [int(cid)]},
                    "FieldNames": ["Id"],
                    "TextAdFieldNames": ["Href"],
                    "MobileAppAdFieldNames": ["TrackingUrl"],
                    "TextImageAdFieldNames": ["Href"],
                    "MobileAppImageAdFieldNames": ["TrackingUrl"],
                    "TextAdBuilderAdFieldNames": ["Href"],
                    "MobileAppAdBuilderAdFieldNames": ["TrackingUrl"],
                    "MobileAppCpcVideoAdBuilderAdFieldNames": ["TrackingUrl"],
                    "CpcVideoAdBuilderAdFieldNames": ["Href"],
                    "CpmBannerAdBuilderAdFieldNames": ["Href"],
                    "CpmVideoAdBuilderAdFieldNames": ["Href"]
                },
            }
            ads = client.ads().post(data=body)
            href = ''
            utm_values = []
# перебираем все объявления, ищем первое с размеченной ссылкой
            for ad in ads().extract():
                utm_values = []
# набор типов объявлений, где ищем Href
                for f in ["TextAd", "TextImageAd", "TextAdBuilderAd", "CpcVideoAdBuilderAd", "CpmBannerAdBuilderAd", "CpmVideoAdBuilderAd"]:
                    if ad.get(f) is not None:
                        href = ad[f]["Href"]
# набор типов объявлений, где ищем TrackingUrl
                for f in ["MobileAppAd", "MobileAppImageAd", "MobileAppAdBuilderAd", "MobileAppCpcVideoAdBuilderAd"]:
                    if ad.get(f) is not None:
                        href = ad[f]["TrackingUrl"]
                if href != '' and href is not None:
# если ссылка найдена - извлекаем из нее метки
                    for utm in ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content']:
                        if href.find(utm) > -1:
                            utm_start = href.find(utm)
                            utm_end = href[href.find(utm):].find('&')
                            if utm_end == -1:
                                utm_end = len(href)
                            else:
                                utm_end += utm_start
# подменяем в метках переменные Яндекс.Директа
                            utm_values.append(href[utm_start + len(utm) + 1:utm_end].replace('{campaign_id}', str(cid)).replace('{campaign_name}', cname))
                        else:
                            utm_values.append('')
# останавливаемся, как только нашли полный набор меток
                if len(utm_values) != 0 and utm_values[0] != '' and utm_values[1] != '' and utm_values[2] != '':
                    break
# метки "по умолчанию" для кампании, финально применятся только после перебора всех объявлений
            if len(utm_values) == 0 or utm_values[0] == utm_values[1] == utm_values[2] == '':
                utm_values = ['yandex', 'cpc', str(cid), '', '']
            items.append([LOGIN, cid, href, utm_values[0], utm_values[1], utm_values[2], utm_values[3], utm_values[4], cname])

# формируем датафрейм из полученных меток
    data = pd.DataFrame(items, columns=["ClientLogin", "CampaignId", "CampaignHref", "UTMSource", "UTMMedium", "UTMCampaign", "UTMTerm", "UTMContent", "CampaignName"])
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["CampaignId"]:
            data[col] = data[col].fillna(0).astype(np.int64)
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
# добавление новых данных
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            try:
                data.to_sql(name=os.getenv('YANDEX_DIRECT_TABLE_UTMS'), con=engine, if_exists='replace', chunksize=100)
                connection.commit()
            except Exception as E:
                print (E)
                connection.rollback()
        elif os.getenv('DB_TYPE') == "CLICKHOUSE":
            requests.post('https://' + os.getenv('DB_HOST') + ':8443/?database=' + os.getenv('DB_DB') + '&query=' + (pd.io.sql.get_schema(data.reset_index(), os.getenv('YANDEX_DIRECT_TABLE_UTMS')) + "  ENGINE=MergeTree ORDER BY (`index`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + os.getenv('DB_PREFIX') + ".").replace("INTEGER", "Int64"),
                headers=auth_post)
            csv_file = data.reset_index().to_csv().encode('utf-8')
            requests.post('https://' + os.getenv('DB_HOST') + ':8443/?database=' + os.getenv('DB_DB') + '&query=INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('YANDEX_DIRECT_TABLE_UTMS') + ' FORMAT CSV',
                headers=auth_post, data=csv_file, stream=True)
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection.close()

    return {
        'statusCode': 200,
        'body': "LoadedCampaigns: " + str(len(data))
    }
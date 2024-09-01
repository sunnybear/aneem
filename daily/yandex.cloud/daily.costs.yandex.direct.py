# Скрипт для ежедневного обновления расходов (и другой статистике по кампаниям) Яндекс.Директ для облачных функций Яндекс.Облака
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * YANDEX_DIRECT_DAYS_UPDATE - период обновления (в днях): 30-90
# * YANDEX_DIRECT_ACCESS_TOKEN - Access Token для приложения, имеющего доступ к статистике нужного кабинета (или несколько - через запятую, порядок как у логина)
# * YANDEX_DIRECT_LOGIN - Логин аккаунта Яндекс.Директа, для которого разрешен доступ к статистике (или несколько - через запятую в том же порядке)
# * YANDEX_DIRECT_TABLE - имя таблицы с статистикой (расходами) кампаний Яндекс.Директа

# requirements.txt:
# pandas
# numpy
# requests
# datetime
# tapi_yandex_direct
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
    date_since = (dt.date.today() - dt.timedelta(days=151)).strftime('%Y-%m-%d')
    date_until = (dt.date.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d')

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
    for i_credentials, TOKEN in enumerate(os.getenv('YANDEX_DIRECT_ACCESS_TOKEN').split(",")):
# нужно ли удалить все данные за 90-150 дней
        data_not_cleaned = True
        TOKEN = TOKEN.strip()
        LOGIN = os.getenv('YANDEX_DIRECT_LOGIN').split(",")[i_credentials].strip()
# формируем запрос для получения метрик яз Яндекс.Директа за последние 150 дней
        api = YandexDirect(
            access_token=TOKEN,
            login=LOGIN,
            is_sandbox=False,
            retry_if_not_enough_units=False,
            language="ru",
            retry_if_exceeded_limit=True,
            retries_if_server_error=5,
            processing_mode="offline",
            wait_report=True,
            return_money_in_micros=False,
            skip_report_header=True,
            skip_column_header=False,
            skip_report_summary=True,
        )
        result = api.reports().post(data={
            "params": {
                "SelectionCriteria": {"DateFrom": date_since, "DateTo": date_until},
                "FieldNames": ["Date", "CampaignId", "CampaignName", "CampaignUrlPath", "ConversionRate", "Conversions", "Clicks", "Cost", "Impressions", "AdNetworkType", "CampaignType", "LocationOfPresenceId", "LocationOfPresenceName", "MobilePlatform", "Device", "ClientLogin"],
                "ReportName": "ActualData" + LOGIN + os.getenv('DB_PREFIX') + date_since + ':' + date_until,
                "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE", # нужно перегружать за 150 дней, иначе расходы не будут соответствовать личному кабинету Директа
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "YES"}
        })
# формируем датафрейм из ответа API
        data = pd.DataFrame(result().to_values(), columns=result.columns)
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["CampaignId", "Clicks", "Conversions", "Impressions","LocationOfPresenceId"]:
                data[col] = data[col].fillna(0).replace('--', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["ConversionRate", "Cost"]:
                data[col] = data[col].fillna(0.0).replace('--', 0.0).astype(float)
# приведение дат
            elif col in ["Date"]:
                data[col] = pd.to_datetime(data[col])
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
            data["ts"] = pd.DatetimeIndex(data["Date"]).asi8
            if data_not_cleaned:
# удаление старых данных
                if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                    try:
                        connection.execute(text("DELETE FROM " + os.getenv('YANDEX_DIRECT_TABLE') + " WHERE `Date`>='" + date_since + "' AND ClientLogin='" + LOGIN + "'"))
                        connection.commit()
                    except Exception as E:
                        print (E)
                        connection.rollback()
                elif os.getenv('DB_TYPE') == "CLICKHOUSE":
# удаление старых данных
                    requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth, verify=cacert,
                        params={"database": os.getenv('DB_DB'), "query": "DELETE FROM " + os.getenv('DB_PREFIX') + "." + os.getenv('YANDEX_DIRECT_TABLE') + " WHERE `Date`>='" + date_since + "' AND ClientLogin='" + LOGIN + "'"})
            data_not_cleaned = False
# добавление новых данных
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                try:
                    data.to_sql(name=os.getenv('YANDEX_DIRECT_TABLE'), con=engine, if_exists='append', chunksize=100)
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                csv_file = data.to_csv().encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443/?database=' + os.getenv('DB_DB') + '&query=INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('YANDEX_DIRECT_TABLE') + ' FORMAT CSV',
                    headers=auth_post, data=csv_file, stream=True)
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            connection.close()
        ret.append('LOGIN => ' + str(len(data)))

    return {
        'statusCode': 200,
        'body': "LoadedCosts: " + ', '.join(ret)
    }
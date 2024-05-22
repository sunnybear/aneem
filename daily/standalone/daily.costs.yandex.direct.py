# Скрипт для ежедневного обновления статистики по кампаниям (включая расходы) из кабинета(-ов) Яндекс.Директа
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * YANDEX_DIRECT.ACCESS_TOKEN - Access Token для приложения, имеющего доступ к статистике нужного сайта (или несколько - через запятую, порядок как у логина)
# * YANDEX_DIRECT.LOGIN - Логин аккаунта Яндекс.Директа, для которого разрешен доступ к статистике (или несколько - через запятую в том же порядке)
# * YANDEX_DIRECT.TABLE - имя результирующей таблицы для статистики (расходов)

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
from tapi_yandex_direct import YandexDirect
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
config.read("../../settings.ini")

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

# перебираем все доступы к рекламным кабинетам
for i_credentials, TOKEN in enumerate(config["YANDEX_DIRECT"]["ACCESS_TOKEN"].split(",")):
    TOKEN = TOKEN.strip()
    LOGIN = config["YANDEX_DIRECT"]["LOGIN"].split(",")[i_credentials].strip()
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

    date_since = (date.today() - timedelta(days=91)).strftime('%Y-%m-%d')
    date_until = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
# Создание запроса на выгрузку данных (помесячно)
    result = client.reports().post(data={
        "params": {
            "SelectionCriteria": {"DateFrom": date_since, "DateTo": date_until},
# список выгружаемых полей
            "FieldNames": [
                "Date",
                "CampaignId",
                "CampaignName",
                "CampaignUrlPath",
                "ClientLogin",
                "ConversionRate",
                "Conversions",
                "Clicks",
                "Cost",
                "Impressions",
                "AdNetworkType",
                "CampaignType",
                "LocationOfPresenceId",
                "LocationOfPresenceName",
                "MobilePlatform",
                "Device"
            ],
# Уникальное имя отчета, чтобы не было дублей с разными данными
            "ReportName": "ActualDataDaily" + str(i_credentials) + date_until,
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "YES"
        }
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
# удаление старых данных
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            try:
                connection.execute(text("DELETE FROM " + config["YANDEX_DIRECT"]["TABLE"] + " WHERE `Date`>='" + date_since + "'"))
                connection.commit()
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": "DELETE FROM " + config["DB"]["DB"] + "." + config["YANDEX_DIRECT"]["TABLE"] + " WHERE `Date`>='" + date_since + "'"},
                headers={'Content-Type':'application/octet-stream'}, verify=False)
# добавление новых данных
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            try:
                data.to_sql(name=config["YANDEX_DIRECT"]["TABLE"], con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_DIRECT"]["TABLE"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (LOGIN + " | " + date_until + ": " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
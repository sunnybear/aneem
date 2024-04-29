# Скрипт для первоначального получения ежедневной статистики по кампаниям (включая расходы) из кабинета Яндекс.Директа
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * YANDEX_DIRECT.ACCESS_TOKEN - Access Token для приложения, имеющего доступ к статистике нужного сайта
# * YANDEX_DIRECT.LOGIN - Логин аккаунта Яндекс.Директа, для которого разрешен доступ к статистике (в случае агентсткого аккаунта нужен отдельный доступ)
# * YANDEX_DIRECT.DELTA - продолжительность периода (в днях) каждой отдельной выгрузки (запроса к API)
# * YANDEX_DIRECT.PERIODS - количество периодов (всех выгрузок), будут выгружены данные за DELTA*PERIODS дней
# * YANDEX_DIRECT.TABLE - имя результирующей таблицы для статистики (расходов)

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
from tapi_yandex_direct import YandexDirect
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine

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

# создаем таблицу для данных при наличии каких-либо данных
table_not_created = True
# создание подключения к API Яндекс.Директ
client = YandexDirect(
  access_token=config["YANDEX_DIRECT"]["ACCESS_TOKEN"],
  login=config["YANDEX_DIRECT"]["LOGIN"],
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

# история Яндекс.Директ, по умолчанию, доступна за 3 года
for period in range(int(config["YANDEX_DIRECT"]["PERIODS"]), 0, -1):
    date_since = (date.today() - timedelta(days=period*int(config["YANDEX_DIRECT"]["DELTA"]))).strftime('%Y-%m-%d')
    date_until = (date.today() - timedelta(days=(period-1)*int(config["YANDEX_DIRECT"]["DELTA"])+1)).strftime('%Y-%m-%d')
# Создание запроса на выгрузку данных (помесячно)
    result = client.reports().post(data={
      "params": {
        "SelectionCriteria": {
          "DateFrom": date_since,
          "DateTo": date_until
        },
# список выгружаемых полей
        "FieldNames": [
          "Date",
          "CampaignId",
          "CampaignName",
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
        "ReportName": "ActualDataInitial" + str(period) + str(date.today().day),
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
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["YANDEX_DIRECT"]["TABLE"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
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
    print (date_since + "=>" + date_until + ": " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
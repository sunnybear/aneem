# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
from tapi_yandex_direct import YandexDirect
import pandas as pd
import requests

# импорт библиотек для работы с БД
import mysql.connector as db_connector
# import psycopg2 as db_connector
# import mariadb as db_connector

# импорт настроек
import configparser
config = configparser.ConfigParser()
config.read("../settings.ini")

# создание подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB"]:
    connection = db_connector.connect(
      host=config["DB"]["HOST"],
      user=config["DB"]["USER"],
      password=config["DB"]["PASSWORD"],
      database=config["DB"]["DB"]
    )
    cursor = connection.cursor()

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
# Создание запроса на выгрузку данных (помесячно)
    result = client.reports().post(data={
      "params": {
        "SelectionCriteria": {
          "DateFrom": (date.today() - timedelta(days=period*int(config["YANDEX_DIRECT"]["DELTA"]))).strftime('%Y-%m-%d'),
          "DateTo": (date.today() - timedelta(days=(period-1)*int(config["YANDEX_DIRECT"]["DELTA"]))+1).strftime('%Y-%m-%d')
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
            data[col] = data[col].fillna(0).replace('--', 0).astype(int)
# приведение вещественных чисел
        elif col in ["ConversionRate", "Cost"]:
            data[col] = data[col].fillna(0.0).replace('--', 0.0).astype(float)
# приведение дат
        elif col in ["Date"]:
            data[col] = pd.to_datetime(data[col])
# приведение строк
        else:
            data[col] = data[col].fillna('')
    data["ts"] = pd.DatetimeIndex(data["Date"]).asi8
# создаем таблицу в первый раз
    if period == int(config["YANDEX_DIRECT"]["PERIODS"]):
        cursor.execute((pd.io.sql.get_schema(data, config["YANDEX_DIRECT"]["TABLE"])).replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "))
        connection.commit()
    data.to_sql(name=config["YANDEX_DIRECT"]["TABLE"], con=connection, if_exists='append')
    connection.commit()

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB"]:
    cursor.close()
    connection.close()
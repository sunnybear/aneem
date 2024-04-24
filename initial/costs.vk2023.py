# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import requests
import time

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

# обновляем токен ВК
r_refresh = requests.post('https://ads.vk.com/api/v2/oauth2/token.json', data={
    'grant_type': 'refresh_token',
    'refresh_token': config["VK_2023"]["REFRESH_TOKEN"],
    'client_id': config["VK_2023"]["CLIENT_ID"],
    'client_secret': config["VK_2023"]["CLIENT_SECRET"]
}).json()
vk2023_access_token = r_refresh['access_token']

# выгружаем данные за 5 лет по месяцам
for period in range(int(config["VK_2023"]["PERIODS"]), 0, -1):
# Задержка в 1 секунду для избежания превышения лимитов по запросам
    time.sleep(1)
# Создание запроса на выгрузку данных (помесячно)
    r = requests.get("https://ads.vk.com/api/v2/statistics/ad_plans/day.json", params={
        'date_from': (date.today() - timedelta(days=period*int(config["VK_2023"]["DELTA"]))).strftime('%Y-%m-%d'),
        'date_to': (date.today() - timedelta(days=(period-1)*int(config["VK_2023"]["DELTA"]))+1).strftime('%Y-%m-%d'),
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
# добавляем метку времени
    data["ts"] = pd.DatetimeIndex(data["date"]).asi8
# базовый процесс очистки: приведение к нужным типам
    for col in data_all.columns:
# приведение целых чисел
        if col in ["shows", "clicks", "goals", "vk_goals", "campaign_id"]:
            data_all[col] = data_all[col].fillna('').replace('', 0).astype(int)
# приведение вещественных чисел
        elif col in ["spent", "cpm", "cpc", "cpa", "ctr", "cr", "vk_cpa", "vk_cr"]:
            data_all[col] = data_all[col].fillna(0.0).astype(float)
# приведение дат
        elif col in ["date"]:
            data_all[col] = pd.to_datetime(data_all[col].fillna("2000-01-01"))
# приведение строк
        else:
            data_all[col] = data_all[col].fillna('')
# создаем таблицу в первый раз
    if period == int(config["VK_2023"]["PERIODS"]):
        cursor.execute((pd.io.sql.get_schema(data, config["VK_2023"]["TABLE"])).replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "))
        connection.commit()
    data.to_sql(name=config["VK_2023"]["TABLE"], con=connection, if_exists='append')
    connection.commit()

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB"]:
    cursor.close()
    connection.close()
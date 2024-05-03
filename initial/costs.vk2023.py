# Скрипт для первоначального получения ежедневной статистики по кампаниям (включая расходы) из кабинета вКонтакте после 2023 года
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * VK2023.CLIENT_SECRET - Client Secret из настроек аккаунта
# * VK2023.CLIENT_ID - Client Id из настроек аккаунта
# * VK2023.REFRESH_TOKEN - Refresh Token (получается после запроса ACCESS TOKEN в API ВК), используется для обновления ACCESS TOKEN
# * VK2023.DELTA - продолжительность периода (в днях) каждой отдельной выгрузки (запроса к API)
# * VK2023.PERIODS - количество периодов (всех выгрузок), будут выгружены данные за DELTA*PERIODS дней
# * VK2023.TABLE - имя результирующей таблицы для статистики (расходов)

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
import time
from sqlalchemy import create_engine, text
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# Скрытие предупреждения Unverified HTTPS request
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

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

# обновляем токен ВК
r_refresh = requests.post('https://ads.vk.com/api/v2/oauth2/token.json', data={
    'grant_type': 'refresh_token',
    'refresh_token': config["VK_2023"]["REFRESH_TOKEN"],
    'client_id': config["VK_2023"]["CLIENT_ID"],
    'client_secret': config["VK_2023"]["CLIENT_SECRET"]
}).json()
vk2023_access_token = r_refresh['access_token']

# создаем таблицу для данных при наличии каких-либо данных
table_not_created = True
# выгружаем данные за 5 лет по месяцам
for period in range(int(config["VK_2023"]["PERIODS"]), 0, -1):
# Задержка в 1 секунду для избежания превышения лимитов по запросам
    time.sleep(1)
    date_since = (date.today() - timedelta(days=period*int(config["VK_2023"]["DELTA"]))).strftime('%Y-%m-%d')
    date_until = (date.today() - timedelta(days=(period-1)*int(config["VK_2023"]["DELTA"]))+1).strftime('%Y-%m-%d')
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
    for col in data_all.columns:
# приведение целых чисел
        if col in ["shows", "clicks", "goals", "vk_goals", "campaign_id"]:
            data_all[col] = data_all[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
        elif col in ["spent", "cpm", "cpc", "cpa", "ctr", "cr", "vk_cpa", "vk_cr"]:
            data_all[col] = data_all[col].fillna(0.0).astype(float)
# приведение дат
        elif col in ["date"]:
            data_all[col] = pd.to_datetime(data_all[col].fillna("2000-01-01"))
# приведение строк
        else:
            data_all[col] = data_all[col].fillna('')
    if len(data):
# добавляем метку времени
        data["ts"] = pd.DatetimeIndex(data["date"]).asi8
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["VK_2023"]["TABLE"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["VK_2023"]["TABLE"], con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["VK_2023"]["TABLE"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (date_since + "=>" + date_until + ": " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
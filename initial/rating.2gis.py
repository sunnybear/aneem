# Скрипт для получения данных рейтинга точек из 2Gis
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.PORT - порт хоста базы данных (если отличается от стандартного)
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * 2GIS.POINTS_RATING - названия точек через запятую
# * 2GIS.LINKS_RATING - ссылки на точки на 2Gis
# * 2GIS.TABLE_RATING - таблица для сохранения данных рейтинга с 2Gis

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
from PIL import Image
from io import StringIO,BytesIO
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
config.read("../settings.ini")

# подключение к БД
if "PORT" in config["DB"] and config["DB"]["PORT"] != '':
    DB_PORT = ':' + config["DB"]["PORT"]
else:
    DB_PORT = ''
if config["DB"]["TYPE"] == "MYSQL":
    engine = create_engine('mysql+mysqldb://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "POSTGRESQL":
    engine = create_engine('postgresql+psycopg2://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"] + '?client_encoding=utf8')
elif config["DB"]["TYPE"] == "MARIADB":
    engine = create_engine('mariadb+mysqldb://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "ORACLE":
    engine = create_engine('oracle+pyodbc://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"])
elif config["DB"]["TYPE"] == "SQLITE":
    engine = create_engine('sqlite:///' + config["DB"]["DB"])

# создание подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection = engine.connect()
    if config["DB"]["TYPE"] in ["MYSQL", "MARIADB"]:
        connection.execute(text('SET NAMES utf8mb4'))
        connection.execute(text('SET CHARACTER SET utf8mb4'))
        connection.execute(text('SET character_set_connection=utf8mb4'))

points = config["2GIS"]["POINTS_RATING"].split(',')
links = config["2GIS"]["LINKS_RATING"].split(',')
rating = []
today = dt.today().strftime("%Y-%m-%d")
rating_start = '"general_rating":'
rating_end = ','

# получаем данные с 2Gis
for i, point in enumerate(points):
# Получаем данные карточки организации
    org = requests.get(links[i], headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 YaBrowser/25.4.0.0 Safari/537.36'})
    rating_text = org.text
    rating_start_pos = rating_text.find(rating_start)
    r = 0.0
# парсим рейтинг
    if rating_start_pos != -1:
        rating_text = rating_text[rating_start_pos + len(rating_start):]
        rating_end_pos = rating_text.find(rating_end)
        if rating_end_pos != -1:
            rating_text = rating_text[:rating_end_pos]
            r = float(rating_text)
    if r > 0:
        rating.append({'date': today, 'point': point, 'rating': r})

data = pd.DataFrame(rating)
if len(data):
# преобразование типов данных
    for col in data.columns:
        if col in ['rating']:
            data[col] = data[col].fillna(0.0).astype(float)
        elif col in ['date']:
            data[col] = pd.to_datetime(data[col])
        else:
            data[col] = data[col].fillna('').astype(str)
# поддержка TCP HTTP для Clickhouse
    if "PORT" in config["DB"] and config["DB"]["PORT"] != '8443':
        CLICKHOUSE_PROTO = 'http://'
        CLICKHOUSE_PORT = config["DB"]["PORT"]
    else:
        CLICKHOUSE_PROTO = 'https://'
        CLICKHOUSE_PORT = '8443'
# создаем таблицу в первый раз
    if config["DB"]["TYPE"] == "CLICKHOUSE":
        requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/', verify=False,
            params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["2GIS"]["TABLE_RATING"]) + "  ENGINE=MergeTree ORDER BY (`" + list(data.columns)[0] + "`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
        try:
            data.to_sql(name=config["2GIS"]["TABLE_RATING"], con=engine, if_exists='append', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
        csv_file = data.to_csv(index=False, header=False).encode('utf-8')
        requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '."' + config["2GIS"]["TABLE_RATING"] + '" FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("2Gis:", len(data))
else:
    print ("2Gis: No data")

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
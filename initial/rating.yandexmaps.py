# Скрипт для получения данных рейтинга точек из Яндекс.Карт
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.PORT - порт хоста базы данных (если отличается от стандартного)
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * YANDEXMAPS.POINTS_RATING - названия точек через запятую
# * YANDEXMAPS.LINKS_RATING - ссылки на точки на Яндекс.Картах
# * YANDEXMAPS.TABLE_RATING - таблица для сохранения данных рейтинга с Яндекс.Карт

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

points = config["YANDEXMAPS"]["POINTS_RATING"].split(',')
links = config["YANDEXMAPS"]["LINKS_RATING"].split(',')
rating = []
today = dt.today().strftime("%Y-%m-%d")

def detect_digit (im):
    digits = [
    [[]],
    [[]],
    [[]],
    [[]],
    [[255, 255, 255, 255, 255, 255, 255, 255, 255, 255], [253, 255, 254, 252, 255, 255, 255, 251, 255, 253], [255, 254, 255, 255, 255, 252, 255, 252, 255, 255], [252, 253, 255, 255, 230,  19,   0, 217, 255, 254], [253, 255, 253, 255, 111,   0,   0, 212, 255, 255], [255, 255, 254, 212,  12,  48,   0, 220, 254, 250], [255, 250, 255,  80,  57, 145,   0, 211, 255, 255], [255, 255, 190,   0, 198, 135,   0, 218, 253, 252], [255, 253,  55,  85, 255, 140,   0, 214, 255, 255], [251, 171,   1, 192, 224, 127,   0, 195, 246, 252], [255,  87,   0,   2,   6,   0,   0,   0, 163, 255], [255, 201, 182, 179, 171,  98,   4, 156, 222, 255], [253, 255, 252, 254, 255, 138,   0, 212, 255, 218], [255, 254, 255, 254, 253, 136,   3, 212, 255, 235], [251, 254, 253, 255, 255, 248, 255, 255, 252, 255], [255, 255, 255, 255, 251, 255, 255, 254, 255, 255], [255, 254, 255, 255, 253, 255, 255, 251, 255, 255]],
    [[255, 255, 255, 255, 255, 255, 255, 255, 255, 255], [251, 255, 254, 255, 255, 255, 254, 255, 249, 255], [254, 255, 255, 250, 255, 255, 255, 254, 255, 252], [255, 221,   0,   0,   2,   0,   1, 113, 255, 255], [253, 230,   1,  76, 110, 116, 110, 171, 251, 255], [255, 220,   0, 167, 255, 254, 255, 255, 255, 255], [249, 227,   0, 175, 245, 238, 255, 248, 255, 254], [255, 221,   0,  41,   0,   1,  85, 243, 253, 255], [255, 221,  19,  82, 159,  99,   0, 104, 252, 255], [255, 252, 254, 255, 254, 255,  40,  26, 253, 255], [253, 255, 254, 252, 255, 251,  69,   0, 253, 254], [255, 223, 244, 255, 255, 235,  18,  45, 255, 253], [255, 153,  33, 108, 115,  43,   0, 172, 255, 253], [253, 230,  81,  15,   4,  42, 179, 255, 254, 252], [255, 249, 253, 253, 255, 255, 255, 255, 255, 254], [248, 255, 255, 251, 253, 255, 253, 248, 255, 255], [255, 254, 252, 255, 254, 255, 253, 255, 254, 251]],
    [[255, 255, 255, 255, 255, 255, 255, 255, 255, 255], [255, 250, 255, 255, 255, 253, 254, 255, 249, 255], [255, 255, 252, 253, 252, 255, 253, 255, 255, 249], [255, 251, 255, 159,  38,   4,  39, 161, 252, 255], [255, 254, 144,   2,  45, 123,  89,  88, 255, 254], [254, 244,  17,  43, 244, 253, 253, 247, 254, 253], [254, 181,   0, 161, 251, 238, 252, 254, 255, 255], [255, 136,   0, 114,  11,   0,  50, 205, 255, 255], [255, 110,   2,  19, 142, 139,   5,  35, 251, 252], [253,  97,   1, 155, 252, 255, 113,   4, 202, 255], [252, 124,   0, 191, 255, 255, 149,   0, 175, 253], [255, 171,   0, 144, 251, 254,  99,   3, 217, 255], [253, 249,  37,  13, 109,  88,   1,  80, 250, 252], [255, 255, 215,  73,  14,  25, 107, 242, 255, 255], [251, 255, 255, 255, 251, 255, 255, 255, 250, 250], [253, 255, 255, 246, 255, 253, 254, 247, 255, 255], [255, 249, 255, 255, 251, 255, 255, 255, 247, 255]],
    [[255, 255, 255, 255, 255, 255, 255, 255, 255, 255], [255, 255, 252, 255, 255, 251, 255, 255, 253, 255], [255, 252, 255, 254, 252, 255, 254, 255, 255, 254], [255, 138,   0,   4,   0,   0,   0,   0, 204, 255], [255, 186, 113, 112, 112, 110,  22,   5, 234, 251], [255, 255, 251, 250, 255, 231,  11,  84, 255, 252], [252, 255, 254, 255, 252, 130,   0, 188, 255, 254], [255, 250, 253, 254, 255,  30,  41, 255, 251, 255], [253, 255, 255, 255, 174,   0, 148, 255, 255, 251], [255, 253, 251, 255,  64,  13, 240, 255, 255, 255], [249, 255, 255, 206,   0,  99, 255, 255, 255, 255], [255, 255, 252, 111,   1, 206, 253, 255, 255, 255], [250, 255, 240,  10,  63, 255, 255, 255, 255, 255], [254, 255, 153,   0, 160, 252, 253, 255, 255, 255], [255, 253, 255, 253, 253, 255, 255, 255, 255, 255], [255, 252, 254, 253, 252, 254, 253, 255, 255, 255], [254, 255, 253, 255, 253, 255, 254, 255, 255, 255]],
    [[255, 255, 255, 255, 255, 255, 255, 255, 255, 255], [255, 255, 255, 251, 255, 252, 255, 255, 255, 254], [254, 250, 254, 255, 255, 255, 251, 255, 255, 255], [250, 255, 222,  77,  14,  24, 107, 240, 255, 251], [255, 249,  42,  13, 110,  85,   1, 107, 249, 255], [255, 220,   0, 108, 254, 255,  48,  21, 255, 255], [255, 212,   5, 139, 255, 249,  71,  24, 254, 248], [247, 255,  38,  38, 212, 191,   7, 109, 254, 255], [255, 252, 217,  20,   0,   2,  50, 246, 255, 248], [255, 235,  39,  51, 174, 158,  18,  82, 255, 250], [255, 141,   3, 203, 255, 254, 132,   2, 201, 255], [255, 127,   0, 186, 255, 248, 116,   2, 195, 253], [254, 209,   5,  25, 117, 106,   0,  33, 248, 254], [255, 255, 185,  49,  15,  21,  88, 220, 252, 255], [255, 248, 255, 255, 252, 254, 250, 255, 255, 255], [255, 255, 249, 252, 255, 255, 255, 255, 248, 255], [255, 255, 255, 255, 254, 252, 255, 254, 255, 253]],
    [[255, 255, 255, 255, 255, 255, 255, 255, 255, 255], [252, 255, 253, 252, 255, 250, 255, 255, 254, 255], [255, 251, 255, 255, 255, 254, 255, 255, 251, 255], [255, 255, 223,  81,   5,  21, 104, 240, 255, 249], [252, 248,  34,   6, 114,  77,   0, 101, 255, 255], [255, 146,   0, 160, 251, 255,  70,   5, 232, 255], [255, 106,   0, 220, 255, 255, 129,   4, 195, 251], [252, 124,   5, 203, 253, 253, 102,   0, 178, 255], [255, 196,   0,  58, 202, 169,   6,   3, 186, 253], [255, 255, 118,   1,   1,   2,  57,   4, 210, 254], [255, 254, 253, 227, 186, 226,  94,   0, 238, 255], [255, 246, 255, 250, 253, 221,  10,  82, 255, 253], [255, 238,  41, 113, 112,  26,  10, 204, 255, 253], [253, 241,  84,  24,   8,  60, 196, 255, 250, 255], [255, 253, 255, 255, 253, 255, 255, 255, 255, 255], [250, 255, 253, 253, 255, 255, 250, 255, 255, 249], [254, 253, 255, 255, 252, 255, 255, 254, 255, 255]]
    ]
    im = np.array(im)
    m = np.sum(im)
    digit = 0
    for i, d in enumerate(digits):
        if len(d) > 1:
            m1 = np.sum(np.absolute(np.array(d) - im))
            if m1 < m:
                digit = i
                m = m1
    return str(digit)

# получаем данные с Яндекс.Карт
for i, point in enumerate(points):
# Получаем скриншот карточки организации
    org = requests.get('https://api.screenshotmachine.com?key=' + config["YANDEXMAPS"]["SCREENSHOTMACHINE_TOKEN"] + '&url=' + links[i] + '&dimension=1024x768')
    im = Image.open(BytesIO(org.content)).convert('L')
# получаем цифры рейтинга
    im1 = im.crop((37, 311, 47, 328))
    im2 = im.crop((49, 311, 59, 328))
# если нашли цифры, то распознаем их по маске
    if np.sum(np.array(im1)) + np.sum(np.array(im2)) < 75000:
        r = float(detect_digit(im1) + '.' + detect_digit(im2))
    else:
# иначе пробуем еще одно место расположения цифр - и тоже распознаем
        im1 = im.crop((101, 311, 111, 328))
        im2 = im.crop((113, 311, 123, 328))
        if np.sum(np.array(im1)) + np.sum(np.array(im2)) < 75000:
            r = float(detect_digit(im1) + '.' + detect_digit(im2))
        else:
            r = 0.0
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
            params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["YANDEXMAPS"]["TABLE_RATING"]) + "  ENGINE=MergeTree ORDER BY (`" + list(data.columns)[0] + "`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
        try:
            data.to_sql(name=config["YANDEXMAPS"]["TABLE_RATING"], con=engine, if_exists='append', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
        csv_file = data.to_csv(index=False, header=False).encode('utf-8')
        requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '."' + config["YANDEXMAPS"]["TABLE_RATING"] + '" FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("Yandex.Maps:", len(data))
else:
    print ("Yandex.Maps: No data")

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
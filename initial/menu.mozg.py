# Скрипт для первоначального получения всех данных по меню из Mozg.rest
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.PORT - порт хоста базы данных (если отличается от стандартного)
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * MOZG.API_ENDPOINT - точка доступа API
# * MOZG.ORGID - ID организации в Mozg.rest
# * MOZG.IMPKEY - Ключ доступа для организации в Mozg.rest
# * MOZG.DBID - ID базы данныд организации в Mozg.rest
# * MOZG.TABLE_MENU - имя результирующей таблицы для меню

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
from io import StringIO
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

# отправка основного запроса
result = requests.post(config['MOZG']['API_ENDPOINT'],
    files = {'orgid': (None, config['MOZG']['ORGID']), 'dbid': (None, config['MOZG']['DBID']), 'impkey': (None, config['MOZG']['IMPKEY']),  'type': (None, 'Items'), 'fields': (None, 'SourceCode,RestCode,Code,Name,Type,Portion,Status,CategoryCode,CategoryType,Category,GroupCode,GroupPath,Group,TicketTime')})

# формируем датафрейм из ответа API
data = pd.read_csv(StringIO(result.text), delimiter=";", names = ['SourceCode', 'RestCode', 'Code', 'Name', 'Type', 'Portion', 'Status', 'CategoryCode', 'CategoryType', 'Category', 'GroupCode', 'GroupPath', 'Group', 'TicketTime'])
# базовый процесс очистки: приведение к нужным типам
for col in data.columns:
# приведение целых чисел
    if col in ["SourceCode", "CategoryCode", "Status"]:
        data[col] = data[col].fillna(0).replace('', 0).astype(np.int64)
# приведение вещественных чисел
    elif col in ["Portion"]:
        data[col] = data[col].fillna(0.0).replace('', 0.0).astype(float)
# приведение строк
    else:
        data[col] = data[col].fillna('')

if len(data):
# поддержка TCP HTTP для Clickhouse
    if 'PORT' in config["DB"] and config["DB"]["PORT"] != '8443':
        CLICKHOUSE_PROTO = 'http://'
        CLICKHOUSE_PORT = config["DB"]["PORT"]
    else:
        CLICKHOUSE_PROTO = 'https://'
        CLICKHOUSE_PORT = '8443'
# создаем таблицy
    if config["DB"]["TYPE"] == "CLICKHOUSE":
        requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/', verify=False,
            params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["MOZG"]["TABLE_MENU"]) + "  ENGINE=MergeTree ORDER BY (`SourceCode`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
        try:
            data.to_sql(name=config["MOZG"]["TABLE_MENU"], con=engine, if_exists='append', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
        csv_file = data.to_csv(index=False).encode('utf-8')
        requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["MOZG"]["TABLE_MENU"] + ' FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("Menu:", len(data))
else:
    print ("No menu")

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["MOZG"]["TABLE_MENU"] + " ADD INDEX dateidx (`Date`)"))
    connection.commit()
    connection.close()
# Скрипт для первоначального получения всей номенклатуры аудитов из Service Inspector
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.PORT - порт хоста базы данных (если отличается от стандартного)
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * SERVICEINSPECTOR.ACCESS_LOGIN - логин доступа к сервису ServiceInspector
# * SERVICEINSPECTOR.ACCESS_PASSWORD - логин доступа к сервису ServiceInspector
# * SERVICEINSPECTOR.TABLE_NOMENCLATURE - имя результирующей таблицы для номенклатуры аудитов

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
if 'PORT' in config["DB"] and config["DB"]["PORT"] != '':
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

# создаем таблицу для данных при наличии каких-либо данных
table_not_created = True
audits = []
date_since = '2000-01-01 00:00:00.000000'
date_until = (date.today()).strftime('%Y-%m-%d 23:59:59.999999')
API_ENDPOINT = 'https://server.serviceinspector.ru'

# отправка запроса на временный токен
try:
    auth_result = requests.get(API_ENDPOINT + '/api/0/auth/access_token?user_login=' + config['SERVICEINSPECTOR']['ACCESS_LOGIN'] + '&user_secret=' + config['SERVICEINSPECTOR']['ACCESS_PASSWORD']).json()
except Exception:
    auth_result = {}

# отправка запроса на данные
if 'accessToken' in auth_result:
    TOKEN = auth_result['accessToken']
    org_id = auth_result['organizationInfo']['id']
    nomenclature_result = requests.get(API_ENDPOINT + '/api/0/inspector/get_nomenclature?access_token=' + TOKEN + '&org_id=' + org_id).json()
    data = pd.DataFrame(nomenclature_result['audits'])
    if len(data):
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["deleted"]:
                data[col] = data[col].fillna(0).replace('', 0).astype(np.int64)
# приведение строк
            else:
                data[col] = data[col].fillna('')
# поддержка TCP HTTP для Clickhouse
        if 'PORT' in config["DB"] and config["DB"]["PORT"] != '8443':
            CLICKHOUSE_PROTO = 'http://'
            CLICKHOUSE_PORT = config["DB"]["PORT"]
        else:
            CLICKHOUSE_PROTO = 'https://'
            CLICKHOUSE_PORT = '8443'
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["SERVICEINSPECTOR"]["TABLE_NOMENCLATURE"]) + "  ENGINE=MergeTree ORDER BY (`id`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["SERVICEINSPECTOR"]["TABLE_NOMENCLATURE"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv(index=False).encode('utf-8')
            requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["SERVICEINSPECTOR"]["TABLE_NOMENCLATURE"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
        print (date_since, "=>", date_until, ":", len(data))
else:
    print ('Ошибка доступа')

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["SERVICEINSPECTOR"]["TABLE_NOMENCLATURE"] + " ADD INDEX ididx (`id`)"))
    connection.commit()
    connection.close()
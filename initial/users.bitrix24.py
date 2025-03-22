# Скрипт для первоначального получения списка пользователей из CRM Битрикс24: crm.user
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * BITRIX24.METHOD - BATCH (для пакетной загрузки) или SINGLE (для одиночной загрузки)
# * BITRIX24.WEBHOOK - URL вебхука (интеграции) Битрикс24
# * BITRIX24.TABLE_USERS - имя результирующей таблицы для crm.user

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
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

# получение количества пользователей
users = requests.get(config["BITRIX24"]["WEBHOOK"] + 'user.get.json?ORDER[ID]=ASC&FILDER[>ID]=0').json()
# общее количество пользователей
users_total = int(users["total"])
# текущий ID контакта - для следующего запроса
last_user_id = 0
# счетчик количества контактов
users_current = 0
# запросы пакетами по 50*50 контактов до исчерпания количества для загрузки
while users_current < users_total:
    users = {}
    if config["BITRIX24"]["METHOD"] == "BATCH":
        cmd = ['cmd[0]=user.get%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D' + str(last_user_id)]
        for i in range(1, 50):
            cmd.append('cmd['+str(i)+']=user.get%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D%24result%5B'+str(i-1)+'%5D%5B49%5D%5BID%5D')
        users_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'batch.json?' + '&'.join(cmd)).json()
# разбор пользователей из пакетного запроса
        for user_group in users_req["result"]["result"]:
            for user in user_group:
                last_user_id = int(user['ID'])
                users[last_user_id] = user
    elif config["BITRIX24"]["METHOD"] == "SINGLE":
        users_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'user.get.json?ORDER[ID]=ASC&FILDER[>ID]=' + str(last_user_id)).json()
# разбор пользователей из обычного запроса
        for user in users_req["result"]:
            last_user_id = int(user['ID'])
            users[last_user_id] = user
    users_current += len(users)
# формируем датафрейм
    data = pd.DataFrame.from_dict(users, orient='index')
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["ID", "XML_ID", "TIME_ZONE_OFFSET", "ACTIVE"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение дат
        elif col in ["LAST_LOGIN", "DATE_REGISTER"]:
            data[col] = pd.to_datetime(data[col].fillna('').replace('', '2000-01-01T00:00:00+03:00').apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%S%z').strftime("%Y-%m-%d %H:%M:%S").replace('202-','2024-')))
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["BITRIX24"]["TABLE_USERS"]) + "  ENGINE=MergeTree ORDER BY (`ID`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            else:
                connection.execute(text("DROP TABLE IF EXISTS " + config["BITRIX24"]["TABLE_USERS"]))
                connection.commit()
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["BITRIX24"]["TABLE_USERS"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv(index=False).encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["BITRIX24"]["TABLE_USERS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (str(last_user_id) + ": " + str(users_current))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
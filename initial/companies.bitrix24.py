# Скрипт для первоначального получения таблицы компаний из CRM Битрикс24: crm.company
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * BITRIX24.METHOD - BATCH (для пакетной загрузки) или SINGLE (для одиночной загрузки)
# * BITRIX24.WEBHOOK - URL вебхука (интеграции) Битрикс24
# * BITRIX24.TABLE_COMPANIES - имя результирующей таблицы для crm.company

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

# получение количества компаний
companies = requests.get(config["BITRIX24"]["WEBHOOK"] + 'crm.company.list.json?ORDER[ID]=ASC&FILDER[>ID]=0').json()
# общее количество компаний
companies_total = int(companies["total"])
# текущий ID компании - для следующего запроса
last_company_id = 0
# счетчик количества компаний
companies_current = 0
# запросы пакетами по 50*50 компаний до исчерпания количества для загрузки
while companies_current < companies_total:
    companies = {}
    if config["BITRIX24"]["METHOD"] == "BATCH":
        cmd = ['cmd[0]=crm.company.list%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D' + str(last_company_id)]
        for i in range(1, 50):
            cmd.append('cmd['+str(i)+']=crm.company.list%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D%24result%5B'+str(i-1)+'%5D%5B49%5D%5BID%5D')
        companies_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'batch.json?' + '&'.join(cmd)).json()
# разбор компаний из пакетного запроса
        for company_group in companies_req["result"]["result"]:
            for company in company_group:
                last_company_id = int(company['ID'])
                companies[last_company_id] = company
    elif config["BITRIX24"]["METHOD"] == "SINGLE":
        companies_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'crm.company.list.json?ORDER[ID]=ASC&FILDER[>ID]=' + str(last_company_id)).json()
# разбор компаний из обычного запроса
        for company in companies_req["result"]:
            last_company_id = int(company['ID'])
            companies[last_company_id] = company
    companies_current += len(companies)
# формируем датафрейм
    data = pd.DataFrame.from_dict(companies, orient='index')
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["ID", "ASSIGNED_BY_ID", "CREATED_BY_ID", "MODIFY_BY_ID", "LEAD_ID", "ADDRESS_LOC_ADDR_ID", "ADDRESS_COUNTRY_CODE", "REG_ADDRESS_COUNTRY_CODE", "REG_ADDRESS_LOC_ADDR_ID", "LAST_ACTIVITY_BY", "SORT", "CATEGORY_ID"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
        elif col in ["REVENUE"]:
# приведение дат
            data[col] = data[col].fillna('').replace('', 0.0).astype(float)
        elif col in ["DATE_CREATE", "DATE_MODIFY", "DATE_CLOSED", "MOVED_TIME", "LAST_ACTIVITY_TIME"]:
            data[col] = pd.to_datetime(data[col].fillna('').replace('', '2000-01-01T00:00:00+03:00').apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%S%z').strftime("%Y-%m-%d %H:%M:%S").replace('202-','2024-')))
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
        data["ts"] = pd.DatetimeIndex(data["DATE_CREATE"]).asi8
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["BITRIX24"]["TABLE_COMPANIES"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            else:
                connection.execute(text("DROP TABLE IF EXISTS " + config["BITRIX24"]["TABLE_COMPANIES"]))
                connection.commit()
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["BITRIX24"]["TABLE_COMPANIES"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["BITRIX24"]["TABLE_COMPANIES"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (str(last_company_id) + ": " + str(companies_current))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
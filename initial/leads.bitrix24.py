# Скрипт для первоначального получения таблицы лидов из CRM Битрикс24: crm.lead
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * BITRIX24.METHOD - BATCH (для пакетной загрузки) или SINGLE (для одиночной загрузки)
# * BITRIX24.WEBHOOK - URL вебхука (интеграции) Битрикс24
# * BITRIX24.TABLE_LEADS - имя результирующей таблицы для crm.lead

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

# получение количества лидов
leads = requests.get(config["BITRIX24"]["WEBHOOK"] + 'crm.lead.list.json?ORDER[ID]=ASC&FILDER[>ID]=0').json()
# общее количество лидов
leads_total = int(leads["total"])
# текущий ID лида - для следующего запроса
last_lead_id = 0
# счетчик количества лидов
leads_current = 0
# запросы пакетами по 50*50 лидов до исчерпания количества для загрузки
while leads_current < leads_total:
    leads = {}
    if config["BITRIX24"]["METHOD"] == "BATCH":
        cmd = ['cmd[0]=crm.lead.list%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D' + str(last_lead_id)]
        for i in range(1, 50):
            cmd.append('cmd['+str(i)+']=crm.lead.list%3Fstart%3D-1%26order%5BID%5D%3DASC%26filter%5B%3EID%5D%3D%24result%5B'+str(i-1)+'%5D%5B49%5D%5BID%5D')
        leads_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'batch.json?' + '&'.join(cmd)).json()
# разбор лидов из пакетного запроса
        for lead_group in leads_req["result"]["result"]:
            for lead in lead_group:
                last_lead_id = int(lead['ID'])
                leads[last_lead_id] = lead
    elif config["BITRIX24"]["METHOD"] == "SINGLE":
        leads_req = requests.get(config["BITRIX24"]["WEBHOOK"] + 'crm.lead.list.json?ORDER[ID]=ASC&FILDER[>ID]=' + str(last_lead_id)).json()
# разбор лидов из обычного запроса
        for lead in leads_req["result"]:
            last_lead_id = int(lead['ID'])
            leads[last_lead_id] = lead
    leads_current += len(leads)
# формируем датафрейм
    data = pd.DataFrame.from_dict(leads, orient='index')
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["COMPANY_ID", "ASSIGNED_BY_ID", "CONTACT_ID", "CREATED_BY_ID", "MODIFY_BY_ID", "MOVED_BY_ID", "ADDRESS_COUNTRY_CODE", "ADDRESS_LOC_ADDR_ID", "LAST_ACTIVITY_BY", "ID"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
        elif col in ["OPPORTUNITY"]:
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
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["BITRIX24"]["TABLE_LEADS"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["BITRIX24"]["TABLE_LEADS"], con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["BITRIX24"]["TABLE_LEADS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (str(last_lead_id) + ": " + str(leads_current))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
# Скрипт для ежедневного удаление компаний, удаленных из AmoCRM
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * AMOCRM.ACESS_TOKEN - ключ доступа Amo.CRM (многоразовый)
# * AMOCRM.TABLE_COMPANIES - имя результирующей таблицы, например, raw_amo_companies

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
config.read("../../settings.ini")

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

# Получение данных для проверки/очистки, получаем по 1000 записей каждые 10 минут
chunk = int((dt.now().timestamp()%86400)//600)*1000
chunk = 0
if config['DB']['TYPE'] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    ids = list(pd.read_sql('SELECT id FROM ' + config['AMOCRM']['TABLE_COMPANIES'] + ' WHERE is_deleted=0 ORDER BY id DESC LIMIT ' + str(chunk) + ', 1000', connection)["id"].values)
elif config['DB']['TYPE'] == "CLICKHOUSE":
    ids = requests.get('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/?query=SELECT id FROM ' + config['DB']['DB'] + '.' + config['AMOCRM']['TABLE_COMPANIES'] + ' FINAL WHERE is_deleted=0 ORDER BY id DESC LIMIT ' + str(chunk) + ', 1000', verify=False).text.strip().split("\n")

# проверяем каждую сделку по id на наличие в Amo.CRM
for id in ids:
    lead = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/companies/' + str(id),
        headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']})
    if len(lead.text) == 0:
# 2 повторные проверки, чтобы исключить случайные/сетевые сбои
        lead = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/companies/' + str(id),
            headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']})
        if len(lead.text) == 0:
            lead = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/companies/' + str(id),
                headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']})
            if len(lead.text) == 0:
                if config['DB']['TYPE'] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                    connection.execute(text('UPDATE ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_COMPANIES"] + ' SET is_deleted=1 WHERE id=' + id))
                    connection.commit()
                elif config['DB']['TYPE'] == "CLICKHOUSE":
                    requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                        params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_COMPANIES"] + ' SELECT * REPLACE(plus(is_deleted, 1) AS is_deleted) FROM ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_COMPANIES"] + 'FINAL WHERE id=' + id})
                print ("Deleted:", id)
	
# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
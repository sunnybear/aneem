# Скрипт для ежедневного обновления воронок и статусов из AmoCRM
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * AMOCRM.ACESS_TOKEN - ключ доступа Amo.CRM (многоразовый)
# * AMOCRM.TABLE_PIPELINES - имя результирующей таблицы, например, raw_amo_pipelines

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

pipelines_exist = True
page = 1
statuses = {}

while pipelines_exist:
    result = False
# отправка запроса
    try:
        result = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/leads/pipelines',
            headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']},
            params = {'limit' : 250, 'page': page}).json()
        if '_links' not in result or 'next' not in result['_links']:
            pipelines_exist = False
    except Exception:
        pipelines_exist = False
# разбор ответа в статусы с полями воронок
    if len(result['_embedded']['pipelines']):
        for l in result['_embedded']['pipelines']:
            for s in l['_embedded']['statuses']:
                status = {}
                for k in s.keys():
                    if k not in ['custom_fields_values', '_links', '_embedded']:
                        status[k] = s[k]
                status['pipeline_name'] = l['name']
                status['pipeline_is_main'] = l['is_main']
                status['pipeline_is_unsorted_on'] = l['is_unsorted_on']
                status['pipeline_is_archive'] = l['is_archive']
                status['pid'] = str(status['pipeline_id']) + '_' + str(status['id'])
                statuses[status['pid']] = status
    page += 1
    print ("Fetched:", len(statuses), page)

if len(statuses):
# формируем датафрейм
    data = pd.DataFrame.from_dict(companies, orient='index')
    ids = ','.join(map(str, list(companies.keys())))
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["id", "account_id", "pipeline_is_main", "pipeline_is_unsorted_on", "pipeline_is_archive", "is_editable", "pipeline_id", "sort", "type"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение дат
        elif col in ["created_at", "updated_at", "closed_at"]:
            data[col] = pd.to_datetime(data[col].fillna(0).replace('None', 0), unit='s')
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data) and 'pid' in list(data.columns):
        data = data.set_index('pid')
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление новых данных
            try:
                data.to_sql(name=config["AMOCRM"]["TABLE_PIPELINES"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
# удаление старых данных
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                params={"database": config["DB"]["DB"], "query": 'DELETE FROM ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_PIPELINES"]})
# добавление новых данных
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_PIPELINES"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("Companies:", str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
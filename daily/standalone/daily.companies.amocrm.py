# Скрипт для ежедневного обновления компаний из AmoCRM
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

companies_exist = True
page = 1
companies = {}
yesterday = round(dt.now().timestamp()) - 90000

while companies_exist:
    result = False
# отправка запроса
    try:
        result = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/companies',
            headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']},
            params = {'limit' : 250, 'page': page, 'filter[updated_at][from]': yesterday}).json()
        if '_links' not in result or 'next' not in result['_links']:
            companies_exist = False
    except Exception:
        companies_exist = False
# разбор ответа в компанию со всеми полями
    if result and len(result['_embedded']['companies']):
        for l in result['_embedded']['companies']:
            company = {}
            for k in l.keys():
                if k not in ['custom_fields_values', '_links', '_embedded']:
                    company[k] = l[k]
                elif l['custom_fields_values']:
                    for f in l['custom_fields_values']:
                        company[f['field_name']] = f['values'][0]['value']
            companies[company['id']] = company
    page += 1
    print ("Fetched:", len(companies), page)

if len(companies):
# формируем датафрейм
    data = pd.DataFrame.from_dict(companies, orient='index')
    ids = ','.join(map(str, list(companies.keys())))
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["id", "pipeline_id", "account_id", "status_id", "company", "group_id", "created_by", "updated_by", "responsible_user_id", "is_deleted", "is_unsorted"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
        elif col in ["price"]:
            data[col] = data[col].fillna('').replace('', 0.0).astype(float)
# приведение дат
        elif col in ["created_at", "updated_at", "closed_at"]:
            data[col] = pd.to_datetime(data[col].fillna(0).replace('None', 0), unit='s')
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data) and 'id' in list(data.columns):
        data = data.set_index('id')
# сверка списка полей
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            current_columns = pd.read_sql("SELECT * FROM " + config["AMOCRM"]["TABLE_COMPANIES"] + " WHERE ID=" + ids[0], connection).columns
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            current_columns = pd.read_csv(StringIO(requests.get('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', params={"database": config["DB"]["DB"], "query": 'SHOW COLUMNS FROM ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_COMPANIES"]}, verify=False).text), delimiter="\t", header=None)[0]
# добавление новых столбцов
        for column_new in set(data.columns)-set(current_columns):
            if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_COMPANIES"] + " ADD COLUMN `" + column_new + "` (TEXT)"))
                connection.commit()
            elif config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'ALTER TABLE ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_COMPANIES"] + ' ADD COLUMN `' + column_new + '` String'}, verify=False)
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# удаление старых данных
            try:
                connection.execute(text("DELETE FROM " + config["AMOCRM"]["TABLE_COMPANIES"] + " WHERE id IN (" + ids  +")"))
                connection.commit()
# добавление новых данных
                try:
                    data.to_sql(name=config["AMOCRM"]["TABLE_COMPANIES"], con=engine, if_exists='append', chunksize=100)
                except Exception as E:
                    print (E)
                    connection.rollback()
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
# удаление старых данных
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                params={"database": config["DB"]["DB"], "query": 'DELETE FROM ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_COMPANIES"] + ' WHERE id IN (' + ids + ')'})
# добавление/замена (ReplacingMergeTree) новых данных
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_COMPANIES"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                params={"database": config["DB"]["DB"], "query": "OPTIMIZE TABLE " + config["DB"]["DB"] + "." + config["AMOCRM"]["TABLE_COMPANIES"])
    print ("Companies:", str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
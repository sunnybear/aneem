# Скрипт для ежедневного обновления товаров из AmoCRM
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * AMOCRM.ACESS_TOKEN - ключ доступа Amo.CRM (многоразовый)
# * AMOCRM.TABLE_PRODUCTS - имя результирующей таблицы, например, raw_amo_products

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

yesterday = round(dt.now().timestamp()) - 90000
products_exists = True
page = 1
products = {}
catalog_id = 0

# отправка запроса на список каталогов
result = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/catalogs',
    headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']},
    params = {'limit' : 250}).json()
if len(result['_embedded']['catalogs']):
    for c in result['_embedded']['catalogs']:
        if c['type'] == 'products':
            catalog_id = c['id']

while products_exists:
    result = False
# отправка запроса
    try:
        result = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/catalogs/' + str(catalog_id) + '/elements',
            headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']},
            , params = {'limit' : 250, 'page': p, 'filter[updated_at][from]': yesterday}).json()
        if '_links' not in result or 'next' not in result['_links']:
            products_exists = False
    except Exception:
        products_exists = False
# разбор ответа в товар со всеми полями
    if len(result['_embedded']['elements']):
        for l in result['_embedded']['elements']:
            product = {}
            for k in l.keys():
                if k not in ['custom_fields_values', '_links', '_embedded', 'rights']:
                    product[k] = l[k]
                elif l['custom_fields_values']:
                    for f in l['custom_fields_values']:
                        product[f['field_name']] = f['values'][0]['value']
            products[product['id']] = product
    page += 1
    print ("Fetched:", len(products), page)

if len(products):
# формируем датафрейм
    data = pd.DataFrame.from_dict(products, orient='index')
    del products
    ids = ','.join(map(str, list(products.keys())))
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["id", "is_deleted", "created_by", "updated_by"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение дат
        elif col in ["created_at", "updated_at"]:
            data[col] = pd.to_datetime(data[col].fillna(0).replace('None', 0), unit='s')
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
        data = data.set_index('id')
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление новых данных
            try:
                data.to_sql(name=config["AMOCRM"]["TABLE_PRODUCTS"], con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
# добавление/замена новых данных
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_PRODUCTS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                params={"database": config["DB"]["DB"], "query": "OPTIMIZE TABLE " + config["DB"]["DB"] + "." + config["AMOCRM"]["TABLE_PRODUCTS"])
    print ("Products:", str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
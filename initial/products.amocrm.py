# Скрипт для первоначального получения таблицы товаров из AmoCRM
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
products_exists = True
page = 1
products = {}
catalog_id = 0

# отправка запроса на список каталогов
result = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/catalogs',
    headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']},
    params = {'limit' : 250, 'page': page}).json()
if len(result['_embedded']['catalogs']):
    for c in result['_embedded']['catalogs']:
        if c['type'] == 'products':
            catalog_id = c['id']

while products_exists and catalog_id:
# отправка запроса
    result = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/catalogs/' + str(catalog_id) + '/elements',
        headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']},
        params = {'limit' : 250, 'page': page}).json()
    if '_links' not in result or 'next' not in result['_links']:
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
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["AMOCRM"]["TABLE_PRODUCTS"]) + "  ENGINE=MergeTree ORDER BY (`id`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            else:
                connection.execute(text("DROP TABLE IF EXISTS " + config["AMOCRM"]["TABLE_PRODUCTS"]))
                connection.commit()
            table_not_created = False
        data = data.set_index('id')
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["AMOCRM"]["TABLE_PRODUCTS"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_PRODUCTS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("Products:", str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_PRODUCTS"] + " ADD INDEX email (`email`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_PRODUCTS"] + " ADD INDEX id (`id`)"))
    connection.commit()
    connection.close()
# Скрипт для первоначального получения таблицы лидов из AmoCRM
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * AMOCRM.ACESS_TOKEN - ключ доступа Amo.CRM (многоразовый)
# * AMOCRM.TABLE_LEADS - имя результирующей таблицы, например, raw_amo_leads

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
leads_exists = True
page = 1
leads = {}

while leads_exists:
# отправка запроса
    result = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/leads',
        headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']},
        params = {'limit' : 250, 'page': page, 'with': 'catalog_elements,contacts'}).json()
    if '_links' not in result or 'next' not in result['_links']:
        leads_exists = False
# разбор ответа в сделку со всеми полями
    if len(result['_embedded']['leads']):
        for l in result['_embedded']['leads']:
            lead = {}
            for k in l.keys():
                if k not in ['custom_fields_values', '_links', '_embedded']:
                    lead[k] = l[k]
                elif k == 'custom_fields_values' and l['custom_fields_values']:
                    for f in l['custom_fields_values']:
                        lead[f['field_name']] = f['values'][0]['value']
                elif k == '_embedded' and l['_embedded']:
                    if 'companies' in l['_embedded'] and len(l['_embedded']['companies']):
                        lead['company'] = l['_embedded']['companies'][0]['id']
                    else:
                        lead['company'] = 0
                    if 'contacts' in l['_embedded'] and len(l['_embedded']['contacts']):
                        lead['contact'] = l['_embedded']['contacts'][0]['id']
                    else:
                        lead['contact'] = 0
                    if 'catalog_elements' in l['_embedded'] and len(l['_embedded']['catalog_elements']):
                        lead['product'] = l['_embedded']['catalog_elements'][0]['id']
                        try:
                            lead['product_catalog'] = l['_embedded']['catalog_elements'][0]['metadata']['catalog_id']
                            lead['product_quantity'] = l['_embedded']['catalog_elements'][0]['metadata']['quantity']
                            lead['product_price_id'] = l['_embedded']['catalog_elements'][0]['metadata']['price_id']
                        except Exception:
                            pass
            leads[lead['id']] = lead
    page += 1
    print ("Fetched:", len(leads), page)

if len(leads):
# формируем датафрейм
    data = pd.DataFrame.from_dict(leads, orient='index')
    del leads
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["id", "pipeline_id", "account_id", "status_id", "company", "group_id", "created_by", "updated_by", "responsible_user_id", "is_deleted", "product", "product_catalog"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
        elif col in ["price", "product_quantity"]:
            data[col] = data[col].fillna('').replace('', 0.0).astype(float)
# приведение дат
        elif col in ["created_at", "updated_at", "closed_at"]:
            data[col] = pd.to_datetime(data[col].fillna(0).replace('None', 0), unit='s')
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": requests.utils.quote((pd.io.sql.get_schema(data, config["AMOCRM"]["TABLE_LEADS"]) + "  ENGINE=ReplacingMergeTree ORDER BY (`id`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64"))})
            else:
                connection.execute(text("DROP TABLE IF EXISTS " + config["AMOCRM"]["TABLE_LEADS"]))
                connection.commit()
            table_not_created = False
        data = data.set_index('id')
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["AMOCRM"]["TABLE_LEADS"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_LEADS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("Leads:", str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_LEADS"] + " ADD INDEX company (`company`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_LEADS"] + " ADD INDEX updated_at (`updated_at`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_LEADS"] + " ADD INDEX created_at (`created_at`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_LEADS"] + " ADD INDEX id (`id`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_LEADS"] + " ADD INDEX status_id (`status_id`)"))
    connection.commit()
    connection.close()
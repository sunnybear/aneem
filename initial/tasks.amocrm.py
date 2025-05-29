# Скрипт для первоначального получения таблицы задач из AmoCRM
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * AMOCRM.ACESS_TOKEN - ключ доступа Amo.CRM (многоразовый)
# * AMOCRM.TABLE_TASKS - имя результирующей таблицы, например, raw_amo_tasks

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
tasks_exists = True
page = 1
tasks = {}

while tasks_exists:
# отправка запроса
    result = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/tasks',
        headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']},
        params = {'limit' : 250, 'page': page}).json()
    if '_links' not in result or 'next' not in result['_links']:
        tasks_exists = False
# разбор ответа в задачу со всеми полями
    if len(result['_embedded']['tasks']):
        for l in result['_embedded']['tasks']:
            task = {'result': ''}
            for k in l.keys():
                if k not in ['_links', '_embedded', 'result']:
                    task[k] = l[k]
                elif k == 'result' and len(l['result']):
                    task['result'] = l['result']['text']
            tasks[task['id']] = task
    page += 1
    print ("Fetched:", len(tasks), page)

if len(tasks):
# формируем датафрейм
    data = pd.DataFrame.from_dict(tasks, orient='index')
    del tasks
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["id", "created_by", "updated_by", "responsible_user_id", "group_id", "entity_id", "duration", "is_completed", "task_type_id", "account_id"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение дат
        elif col in ["created_at", "updated_at", "complete_till"]:
            data[col] = pd.to_datetime(data[col].fillna(0).replace('None', 0), unit='s')
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["AMOCRM"]["TABLE_TASKS"]) + "  ENGINE=ReplacingMergeTree ORDER BY (`id`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            else:
                connection.execute(text("DROP TABLE IF EXISTS " + config["AMOCRM"]["TABLE_TASKS"]))
                connection.commit()
            table_not_created = False
        data = data.set_index('id')
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["AMOCRM"]["TABLE_TASKS"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_TASKS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("Tasks:", str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_TASKS"] + " ADD INDEX responsible_user_id (`responsible_user_id`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_TASKS"] + " ADD INDEX updated_at (`updated_at`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_TASKS"] + " ADD INDEX created_at (`created_at`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_TASKS"] + " ADD INDEX id (`id`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_TASKS"] + " ADD INDEX is_completed (`is_completed`)"))
    connection.commit()
    connection.close()
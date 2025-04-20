# Скрипт для первоначального получения таблицы пользователей из AmoCRM
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * AMOCRM.ACESS_TOKEN - ключ доступа Amo.CRM (многоразовый)
# * AMOCRM.TABLE_USERS - имя результирующей таблицы, например, raw_amo_users

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
users_exists = True
page = 1
users = {}

while users_exists:
# отправка запроса
    result = requests.get('https://' + config['AMOCRM']['INSTANCE'] + '/api/v4/users',
        headers = {'Authorization': 'Bearer ' + config['AMOCRM']['ACCESS_TOKEN']},
        params = {'limit' : 250, 'page': page}).json()
    if '_links' not in result or 'next' not in result['_links']:
        users_exists = False
# разбор ответа в пользователя со всеми полями
    if len(result['_embedded']['users']):
        for l in result['_embedded']['users']:
            user = {}
            for k in l.keys():
                if k not in ['custom_fields_values', '_links', '_embedded', 'rights']:
                    user[k] = l[k]
                elif k == 'custom_fields_values':
                    for f in l['custom_fields_values']:
                        user[f['field_name']] = f['values'][0]['value']
                elif k == '_embedded':
                    if len(l['_embedded']['roles']):
                        user['role'] = l['_embedded']['roles'][0]['id']
                        user['role_name'] = l['_embedded']['roles'][0]['name']
                    else:
                        user['role'] = 0
                    if len(l['_embedded']['groups']):
                        user['group'] = l['_embedded']['groups'][0]['id']
                        user['group_name'] = l['_embedded']['groups'][0]['name']
                    else:
                        user['group'] = 0
                elif k == 'rights':
                    for rights_group in l[k].keys():
                        if rights_group in ['leads', 'contacts', 'companies', 'tasks']:
                            for right in l[k][rights_group].keys():
                                user['rights_' + rights_group + '_' + right] = l[k][rights_group][right]
                        elif rights_group in ['status_rights', 'catalog_rights']:
                            for rights_group_status in l[k][rights_group]:
                                if 'status_id' in rights_group_status:
                                    rights_group_prefix = 'status'
                                    rights_group_key = 'status_id'
                                elif 'catalog_id' in rights_group_status:
                                    rights_group_prefix = 'catalog'
                                    rights_group_key = 'catalog_id'
                                for status_key in rights_group_status.keys():
                                    if status_key == 'rights':
                                        for status_right in rights_group_status[status_key].keys():
                                            user['rights_' + rights_group_prefix + '_' + str(rights_group_status[rights_group_key]) + '_rights_' + status_right] = rights_group_status[status_key][status_right]
                                    else:
                                        user['rights_' + rights_group_prefix + '_' + str(rights_group_status[rights_group_key]) + '_' + status_key] = rights_group_status[status_key]
                        else:
                            user['rights_' + rights_group] = l[k][rights_group]
            users[user['id']] = user
    page += 1
    print ("Fetched:", len(users), page)

if len(users):
# формируем датафрейм
    data = pd.DataFrame.from_dict(users, orient='index')
    del users
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["id", "rights_mail_access", "rights_catalog_access", "rights_files_access", "rights_oper_day_reports_view_access", "rights_oper_day_user_tracking", "rights_is_admin", "rights_is_free", "rights_is_active", "rights_group_id", "rights_role_id"]:
            data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["AMOCRM"]["TABLE_USERS"]) + "  ENGINE=MergeTree ORDER BY (`id`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            else:
                connection.execute(text("DROP TABLE IF EXISTS " + config["AMOCRM"]["TABLE_USERS"]))
                connection.commit()
            table_not_created = False
        data = data.set_index('id')
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["AMOCRM"]["TABLE_USERS"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["AMOCRM"]["TABLE_USERS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("Users:", str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_USERS"] + " ADD INDEX email (`email`)"))
    connection.execute(text("ALTER TABLE " + config["AMOCRM"]["TABLE_USERS"] + " ADD INDEX id (`id`)"))
    connection.commit()
    connection.close()
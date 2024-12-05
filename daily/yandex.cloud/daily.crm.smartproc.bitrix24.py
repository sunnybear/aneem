# Скрипт для регулярного получения данных смарт-процессов CRM Битрикс24 через приложение "Импорт и экспорт смарт-процессов": https://www.bitrix24.ru/apps/app/archeon.import_i_eksport_smart_protsessov/
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * BITRIX24_SMARTPROC_COOKIE_ARCHEON - Cookie для страницы smart-process-import24.archeon.io/dashboard/
# * BITRIX24_TABLE_SMARTPROC - базовое имя результирующей таблицы для смарт-процессов
# * BITRIX24_SMARTPROC_IDS - ID смарт процессов для выгрузки (пусто, если выгружать все), через запятую

# requirements.txt:
# pandas
# numpy
# requests
# datetime
# openpyxl
# sqlalchemy
# bs4

# timeout: 300
# memory: 1024

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
import time
import os
from io import BytesIO
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup

def handler(event, context):
    auth = {
        'X-ClickHouse-User': os.getenv('DB_USER'),
        'X-ClickHouse-Key': context.token["access_token"]
    }
    auth_post = auth.copy()
    auth_post['Content-Type'] = 'application/octet-stream'
    cacert = '/etc/ssl/certs/ca-certificates.crt'

# подключение к БД
    if os.getenv('DB_TYPE') == "MYSQL":
        engine = create_engine('mysql+mysqldb://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB') + '?charset=utf8')
    elif os.getenv('DB_TYPE') == "POSTGRESQL":
        engine = create_engine('postgresql+psycopg2://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB') + '?client_encoding=utf8')
    elif os.getenv('DB_TYPE') == "MARIADB":
        engine = create_engine('mariadb+mysqldb://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB') + '?charset=utf8')
    elif os.getenv('DB_TYPE') == "ORACLE":
        engine = create_engine('oracle+pyodbc://' + os.getenv('DB_USER') + ':' + os.getenv('DB_PASSWORD') + '@' + os.getenv('DB_HOST') + '/' + os.getenv('DB_DB'))
    elif os.getenv('DB_TYPE') == "SQLITE":
        engine = create_engine('sqlite:///' + os.getenv('DB_DB'))

# создание подключения к БД
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection = engine.connect()
        if os.getenv('DB_TYPE') in ["MYSQL", "MARIADB"]:
            connection.execute(text('SET NAMES utf8mb4'))
            connection.execute(text('SET CHARACTER SET utf8mb4'))
            connection.execute(text('SET character_set_connection=utf8mb4'))

# загрузка страницы для запросов данных смарт-процессов
    headers = {
        'User-Agent': 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36',
        'Cookie': os.getenv('BITRIX24_SMARTPROC_COOKIE_ARCHEON')
    }
    response_base = requests.get('https://smart-process-import24.archeon.io/dashboard/', headers=headers)
    soup = BeautifulSoup(response_base.text, 'html.parser')

# получение промежуточного токена
    csrf_token = soup.find_all(attrs={"name":"csrfmiddlewaretoken"})[0].get("value")

    smartproc_ids = os.getenv('BITRIX24_SMARTPROC_IDS').split(",")
# получение списка ID всех смарт процессов, если конкретный(-е) не заданы
    if len(smartproc_ids) == 0:
        smartproc_ids = []
        for crm_type_option in soup.find_all(attrs={"name":"crm_type"})[0].children:
            smartproc_ids.append(crm_type_option.get("value"))

    ret = []
# отправка запроса на получение данных всех смарт-процессов
    for smartproc_id in smartproc_ids:
        requests.post('https://smart-process-import24.archeon.io/dashboard/', headers=headers, data={
            'csrfmiddlewaretoken': csrf_token,
            'crm_type': smartproc_id,
            'task_type': 'export'
        })
# выгрузка результатов
        result_not_ready = True
        while result_not_ready:
            time.sleep(5)
            response_result = requests.get('https://smart-process-import24.archeon.io/tasks/', headers=headers)
            soup = BeautifulSoup(response_result.text, 'html.parser')
            download_link = soup.find_all("tbody")[0].find_all("tr")[0].find_all("td")[5].find("a")
            if download_link:
                result_not_ready = False
                response = requests.get(download_link.get("href"), headers=headers)

        if response and len(response.content):
# формируем датафрейм
            data = pd.read_excel(BytesIO(response.content))
# запоминаем названия полей для представления (view) этого смарт-процесса
            column_titles = pd.DataFrame(data.iloc[:1])
            sql_view_template = []
            for column_title in column_titles.columns:
                sql_view_template.append('`' + column_title + '` AS `' + column_titles[column_title].values[0] + '`')
# удаляем первую строку - с именами полей
            data = data.iloc[1:]
# базовый процесс очистки: приведение к нужным типам
            for col in data.columns:
# приведение целых чисел
                if col in ["id"]:
                    data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение дат
                elif col in ["createdTime", "updatedTime", "lastActivityTime"]:
                    data[col] = pd.to_datetime(data[col].fillna('').replace('', '2000-01-01T00:00:00+03:00').apply(lambda x: dt.strptime(x, '%Y-%m-%dT%H:%M:%S%z').strftime("%Y-%m-%d %H:%M:%S").replace('202-','2024-')))
# приведение строк
                else:
                    data[col] = data[col].fillna('')
        if len(data):
            if "createdTime" in data.columns:
                data["ts"] = pd.DatetimeIndex(data["createdTime"]).asi8
                index = 'ts'
            else:
                index = 'id'
            table = os.getenv('BITRIX24_TABLE_SMARTPROC') + smartproc_id
# создаем таблицу в первый раз
            if os.getenv('DB_TYPE') == "CLICKHOUSE":
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": (pd.io.sql.get_schema(data, table) + "  ENGINE=MergeTree ORDER BY (`" + index + "`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + os.getenv('DB_PREFIX') + ".").replace("INTEGER", "Int64")})
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
                try:
                    data.to_sql(name=table, con=engine, if_exists='replace', chunksize=100)
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
# создаем представление по добавленным данным
                try:
                    connection.execute(text("CREATE VIEW " + table + "_view AS SELECT " + ",".join(sql_view_template) + " FROM " + table))
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                csv_file = data.to_csv(index=False).encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": 'INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + table + ' FORMAT CSV'},
                    data=csv_file, stream=True)
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": "CREATE OR REPLACE VIEW " + os.getenv('DB_DB') + "." + table + "_view AS SELECT " + ",".join(sql_view_template) + " FROM " + os.getenv('DB_DB') + "." + table})
        ret.append(smartproc_id + " => " + str(len(data)))

# закрытие подключения к БД
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection.close()

    return {
        'statusCode': 200,
        'body': "LoadedSmartProcesses: " + ','.join(ret)
    }
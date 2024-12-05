# Скрипт для первоначального получения данных смарт-процессов CRM Битрикс24 через приложение "Импорт и экспорт смарт-процессов": https://www.bitrix24.ru/apps/app/archeon.import_i_eksport_smart_protsessov/
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * BITRIX24.SMARTPROC_COOKIE_ARCHEON - Cookie для страницы smart-process-import24.archeon.io/dashboard/
# * BITRIX24.TABLE_SMARTPROC - базовое имя результирующей таблицы для смарт-процессов
# * BITRIX24.SMARTPROC_IDS - ID смарт процессов для выгрузки (пусто, если выгружать все), через запятую

# !pip install openpyxl
# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
import time
from io import BytesIO
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
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

# загрузка страницы для запросов данных смарт-процессов
headers = {
    'User-Agent': 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36',
    'Cookie': config["BITRIX24"]["SMARTPROC_COOKIE_ARCHEON"]
}
response_base = requests.get('https://smart-process-import24.archeon.io/dashboard/', headers=headers)
soup = BeautifulSoup(response_base.text, 'html.parser')

# получение промежуточного токена
csrf_token = soup.find_all(attrs={"name":"csrfmiddlewaretoken"})[0].get("value")

smartproc_ids = config["BITRIX24"]["SMARTPROC_IDS"].split(",")
# получение списка ID всех смарт процессов, если конкретный(-е) не заданы
if len(smartproc_ids) == 0:
    smartproc_ids = []
    for crm_type_option in soup.find_all(attrs={"name":"crm_type"})[0].children:
        smartproc_ids.append(crm_type_option.get("value"))

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
        if config["DB"]["TYPE"] == "CLICKHOUSE":
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, table) + "  ENGINE=MergeTree ORDER BY (`" + index + "`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=table, con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
# создаем представление по добавленным данным
            try:
                connection.execute(text("CREATE VIEW " + table + "_view AS SELECT " + ",".join(sql_view_template) + " FROM " + table))
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv(index=False).encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": "INSERT INTO " + config["DB"]["DB"] + "." + table + " FORMAT CSV"},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                params={"database": config["DB"]["DB"], "query": "CREATE OR REPLACE VIEW " + config["DB"]["DB"] + "." + table + "_view AS SELECT " + ",".join(sql_view_template) + " FROM " + config["DB"]["DB"] + "." + table})
    print (smartproc_id + " => " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
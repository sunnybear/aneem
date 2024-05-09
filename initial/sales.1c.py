# Скрипт для первоначального получения продаж из выгрузок 1с
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * 1С.ROOT - папка с выгрузками из 1С
# * 1С.DELIMITER - разделитель полей в CSV/TSV файлах
# * 1С.ENCODING - кодировка CSV/TSV файлов
# * 1C.TABLE_SALES - имя результирующей таблицы для продаж
# * 1C.TABLE_SALES_INDEX - имя индексного поля результирующей таблицы для продаж

# импорт общих библиотек
import os
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

# разбираем все файлы, которые выгружены из 1С
data = pd.DataFrame()
for f in os.listdir(config["1C"]["ROOT"]):
    f = os.path.join(config["1C"]["ROOT"], f)
    if os.path.isfile(f):
        sales_tmp = pd.read_csv(f, encoding=config["1C"]["ENCODING"], delimiter=config["1C"]["DELIMITER"])
        if len(data):
# исключаем из старых данных новые (обновленные) записи по заданному индексу
            data = pd.concat([data[~data[config["1C"]["TABLE_SALES_INDEX"]].isin(sales_tmp[config["1C"]["TABLE_SALES_INDEX"]].values)], sales_tmp])
        else:
            data = pd.DataFrame(sales_tmp)
if len(data):
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in [""]:
            data[col] = data[col].fillna('').replace('None', '').replace('', 0).astype(np.int64)
# приведение вещественных чисел
        elif col in ["Цена", "Сумма"]:
            data[col] = data[col].fillna('').replace('', 0.0).apply(lambda x: ''.join(c if c.isdigit() or c=='.' or c==',' else '' for c in x)).replace(',','.').astype(float)
        elif col in ["Дата_Заказа", "Дата_Реализации"]:
# приведение дат
            data[col] = pd.to_datetime(data[col].fillna('').replace('None', '').replace('', '01.01.2000').apply(lambda x: dt.strptime(x, '%d.%m.%Y').strftime("%Y-%m-%d %H:%M:%S") if len(x.split("."))>2 else '2000-01-01 00:00:00'))
# приведение строк
        else:
            data[col] = data[col].fillna('')
# создаем таблицу в первый раз
    if table_not_created:
        if config["DB"]["TYPE"] == "CLICKHOUSE":
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["1C"]["TABLE_SALES"]) + "  ENGINE=MergeTree ORDER BY (`" + config["1C"]["TABLE_SALES_INDEX"] + "`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
        table_not_created = False
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
        try:
            data.to_sql(name=config["1C"]["TABLE_SALES"], con=engine, if_exists='append', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
        csv_file = data.to_csv().encode('utf-8')
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["1C"]["TABLE_SALES"] + ' FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
print (str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# создаем индексы
    connection.execute(text("ALTER TABLE " + config["1C"]["TABLE_SALES"] + " ADD INDEX idx (`" + config["1C"]["TABLE_SALES_INDEX"] + "`)"))
    if "Дата_Заказа" in data.columns:
        connection.execute(text("ALTER TABLE " + config["1C"]["TABLE_SALES"] + " ADD INDEX idx_date1 (`Дата_Заказа`)"))
    if "Дата_Реализации" in data.columns:
        connection.execute(text("ALTER TABLE " + config["1C"]["TABLE_SALES"] + " ADD INDEX idx_date2 (`Дата_Реализации`)"))
    connection.commit()
    connection.close()
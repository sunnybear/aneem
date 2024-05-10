# Скрипт для ежедневного обновления данных о расходах из Яндекс.Метрики (за вчера)
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * YANDEX_METRIKA.ACCESS_TOKEN - Access Token для приложения, имеющего доступ к статистике нужного сайта
# * YANDEX_METRIKA.COUNTER_ID - ID сайта, статистику которого нужно выгрузить
# * YANDEX_METRIKA.TABLE_COSTS - имя результирующей таблицы для расходов

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import requests
import time
from tapi_yandex_metrika import YandexMetrikaStats
import numpy as np
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

api = YandexMetrikaStats(access_token=config["YANDEX_METRIKA"]["ACCESS_TOKEN"])
# Создание запроса на выгрузку данных (30 дней назад)
yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
yesterday_1 = (date.today() - timedelta(days=30)).strftime('%Y-%m-%d')
params = {
    "ids": config["YANDEX_METRIKA"]["COUNTER_ID"],
    "metrics": "ym:ev:expensesRUB,ym:ev:visits,ym:ev:expenseClicks",
    "dimensions": "ym:ev:date,ym:ev:lastExpenseSource,ym:ev:lastExpenseMedium,ym:ev:lastExpenseCampaign",
    "date1": yesterday_1,
    "date2": yesterday,
    "limit": 100000
}
# отправляем запрос API
result = api.stats().get(params=params)
# формируем датафрейм по отдельности из каждой части
data = pd.DataFrame(result().to_values(), columns=result.columns)
# базовый процесс очистки: приведение к нужным типам
for col in data.columns:
# приведение целых чисел
    if col in ["ym:ev:visits", "ym:ev:expenseClicks"]:
        data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
    elif col in ["ym:ev:expensesRUB"]:
        data[col] = data[col].fillna(0.0).astype(float)
# приведение дат
    elif col in ["ym:ev:date"]:
        data[col] = pd.to_datetime(data[col].fillna("2000-01-01").apply(lambda x: dt.strptime(x, "%Y-%m-%d")))
# приведение строк
    else:
        data[col] = data[col].fillna('')
if len(data):
# добавляем метку времени
    data["ts"] = pd.DatetimeIndex(data["ym:ev:date"]).asi8
    if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обновление данных о расходах
        try:
            connection.execute(text("DELETE FROM " + config["YANDEX_METRIKA"]["TABLE_COSTS"] + " WHERE `ym:ev:date`>='" + yesterday_1 + "'"))
            connection.commit()
        except Exception as E:
            print (E)
            connection.rollback()
        try:
            data.to_sql(name=config["YANDEX_METRIKA"]["TABLE_COSTS"], con=engine, if_exists='append', chunksize=100)
        except Exception as E:
            print (E)
            connection.rollback()
    elif config["DB"]["TYPE"] == "CLICKHOUSE":
# удаляем данные за вчера
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
            params={"database": config["DB"]["DB"], "query": "DELETE FROM " + config["DB"]["DB"] + "." + config["YANDEX_METRIKA"]["TABLE_COSTS"] + " WHERE `ym:ev:date`>='" + yesterday_1 + "'"}, headers={'Content-Type':'application/octet-stream'}, verify=False)
# добавляем новые данные
        csv_file = data.to_csv(index=False).encode('utf-8')
        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
            params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_METRIKA"]["TABLE_COSTS"] + ' FORMAT CSV'},
            headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
# удаляем обработанный запрос из API
    print (yesterday + ": " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
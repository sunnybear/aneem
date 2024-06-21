# Скрипт для ежедневного обновления данных Яндекс.Метрики (за вчера)
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * YANDEX_METRIKA.ACCESS_TOKEN - Access Token для приложения, имеющего доступ к статистике нужного сайта. Если несколько сайтов, то для каждого указать (скопировать) через запятую
# * YANDEX_METRIKA.COUNTER_ID - ID сайта, статистику которого нужно выгрузить. Можно несколько через запятую, если несколько сайтов
# * YANDEX_METRIKA.TABLE_VISITS - имя результирующей таблицы для визитов (сессий)
# * YANDEX_METRIKA.TABLE_VISITS_GOALS - имя результирующей таблицы для целей, при необходимости

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import requests
import time
from tapi_yandex_metrika import YandexMetrikaLogsapi
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

# метка очистки старых данных
data2clean = True
# перебираем токены Яндекс.Метрики
for i_credentials, TOKEN in enumerate(config["YANDEX_METRIKA"]["ACCESS_TOKEN"].split(",")):
    TOKEN = TOKEN.strip()
    COUNTER_ID = config["YANDEX_METRIKA"]["COUNTER_ID"].split(",")[i_credentials].strip()
    api = YandexMetrikaLogsapi(access_token=TOKEN, default_url_params={'counterId': COUNTER_ID})
# Создание запроса на выгрузку данных (вчера + позавчера)
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    yesterday_1 = (date.today() - timedelta(days=2)).strftime('%Y-%m-%d')
    params = {
        "fields": "ym:s:visitID,ym:s:dateTime,ym:s:isNewUser,ym:s:startURL,ym:s:endURL,ym:s:pageViews,ym:s:visitDuration,ym:s:ipAddress,ym:s:regionCountry,ym:s:regionCity,ym:s:regionCountryID,ym:s:regionCityID,ym:s:clientID,ym:s:networkType,ym:s:goalsID,ym:s:goalsDateTime,ym:s:goalsPrice,ym:s:goalsOrder,ym:s:referer,ym:s:from,ym:s:lastTrafficSource,ym:s:lastAdvEngine,ym:s:lastReferalSource,ym:s:lastSearchEngineRoot,ym:s:lastSearchEngine,ym:s:lastSocialNetwork,ym:s:lastSocialNetworkProfile,ym:s:lastDirectClickOrder,ym:s:lastDirectPlatformType,ym:s:lastDirectPlatform,ym:s:lastUTMCampaign,ym:s:lastUTMContent,ym:s:lastUTMMedium,ym:s:lastUTMSource,ym:s:lastUTMTerm,ym:s:automaticUTMCampaign,ym:s:automaticUTMContent,ym:s:automaticUTMMedium,ym:s:automaticUTMSource,ym:s:automaticUTMTerm,ym:s:lastRecommendationSystem,ym:s:lastGCLID,ym:s:lastMessenger,ym:s:browser,ym:s:browserLanguage,ym:s:browserCountry,ym:s:deviceCategory,ym:s:mobilePhone,ym:s:mobilePhoneModel,ym:s:operatingSystemRoot,ym:s:operatingSystem,ym:s:browserMajorVersion,ym:s:browserMinorVersion,ym:s:browserEngine,ym:s:browserEngineVersion1,ym:s:browserEngineVersion2,ym:s:browserEngineVersion3,ym:s:browserEngineVersion4",
        "source": "visits",
        "date1": yesterday_1,
        "date2": yesterday
}
# удаляем все текущие запросы API
    api_requests = api.allinfo().get()
    if "requests" in api_requests:
        for req in api_requests["requests"]:
            if req["status"] == "processed":
                try:
                    api.clean(requestId=req["request_id"]).post()
                except Exception as E:
                    continue# отправляем запрос API
    result = api.create().post(params=params)
# получаем ID в очереди
    request_id = result["log_request"]["request_id"]
    info = api.info(requestId=request_id).get()
# ждем выполнения запроса
    while info["log_request"]["status"] != "processed":
        time.sleep(10)
        info = api.info(requestId=request_id).get()
# суммарное число сессий и целей - для статистики
    visits_total = 0
    goals_total = 0
# обрабатываем результат запроса
    for p in info["log_request"]["parts"]:
        part = api.download(requestId=request_id, partNumber=p["part_number"]).get()
# формируем датафрейм по отдельности из каждой части
        data = pd.DataFrame(part().to_values(), columns=part.columns)
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["ym:s:isNewUser", "ym:s:pageViews", "ym:s:regionCountryID", "ym:s:regionCityID", "ym:s:browserMajorVersion", "ym:s:browserMinorVersion", "ym:s:browserEngineVersion1", "ym:s:browserEngineVersion2", "ym:s:browserEngineVersion3", "ym:s:browserEngineVersion4"]:
                data[col] = data[col].fillna('').replace('', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["ym:s:visitDuration"]:
                data[col] = data[col].fillna(0.0).astype(float)
# приведение дат
            elif col in ["ym:s:dateTime"]:
                data[col] = pd.to_datetime(data[col].fillna("2000-01-01 00:00:00").apply(lambda x: dt.strptime(x, "%Y-%m-%d %H:%M:%S")))
# приведение строк
            else:
                data[col] = data[col].fillna('')
# извлекаем информацию о целях
        goals = []
        if len(data) and config["YANDEX_METRIKA"]["TABLE_VISITS_GOALS"] != "":
            for i, row in data[data["ym:s:goalsID"] != '[]'].iterrows():
                goals_id = row["ym:s:goalsID"].replace('[', '').replace(']', '').split(",")
                goals_dt = row["ym:s:goalsDateTime"].replace('[', '').replace(']', '').replace("\\'", '').split(",")
                goals_price = row["ym:s:goalsPrice"].replace('[', '').replace(']', '').replace("'", '').split(",")
                goals_order = row["ym:s:goalsOrder"].replace('[', '').replace(']', '').replace("'", '').split(",")
                for j, goal in enumerate(goals_id):
                    goals.append([row["ym:s:visitID"], row["ym:s:clientID"], goal, goals_dt[j], goals_price[j], goals_order[j]])
            goals = pd.DataFrame(goals)
            goals.columns = ['ym:s:visitID', 'ym:s:clientID', 'ym:s:goalID', 'ym:s:goalDateTime', 'ym:s:goalPrice', 'ym:s:goalOrder']
            goals["ym:s:goalDateTime"] = pd.to_datetime(goals["ym:s:goalDateTime"])
            goals["ym:s:goalPrice"] = goals["ym:s:goalPrice"].fillna(0.0).astype(float)
            goals["ts"] = pd.DatetimeIndex(goals["ym:s:goalDateTime"]).asi8
        if len(data):
# добавляем метку времени
            data["ts"] = pd.DatetimeIndex(data["ym:s:dateTime"]).asi8
            if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обновление данных о визитах
                try:
                    connection.execute(text("DELETE FROM " + config["YANDEX_METRIKA"]["TABLE_VISITS"] + " WHERE `ym:s:dateTime`>='" + yesterday_1 + "'"))
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
                try:
                    data.to_sql(name=config["YANDEX_METRIKA"]["TABLE_VISITS"], con=engine, if_exists='append', chunksize=100)
                except Exception as E:
                    print (E)
                    connection.rollback()
# обновление данных о целях
                if len(goals):
                    try:
                        connection.execute(text("DELETE FROM " + config["YANDEX_METRIKA"]["TABLE_VISITS_GOALS"] + " WHERE `ym:s:goalDateTime`>='" + yesterday_1 + "'"))
                        connection.commit()
                    except Exception as E:
                        print (E)
                        connection.rollback()
                    try:
                        goals.to_sql(name=config["YANDEX_METRIKA"]["TABLE_VISITS_GOALS"], con=engine, if_exists='append', chunksize=100)
                    except Exception as E:
                        print (E)
                        connection.rollback()
            elif config["DB"]["TYPE"] == "CLICKHOUSE":
                if data2clean:
# удаляем данные за вчера, сессии
                    requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                        params={"database": config["DB"]["DB"], "query": "DELETE FROM " + config["DB"]["DB"] + "." + config["YANDEX_METRIKA"]["TABLE_VISITS"] + " WHERE `ym:s:dateTime`>='" + yesterday_1 + "'"}, headers={'Content-Type':'application/octet-stream'}, verify=False)
# добавляем новые данные, сессии
                csv_file = data.to_csv(index=False).encode('utf-8')
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                    params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_METRIKA"]["TABLE_VISITS"] + ' FORMAT CSV'},
                    headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
                if len(goals):
                    if data2clean:
# удаляем данные за вчера, цели
                        requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                            params={"database": config["DB"]["DB"], "query": "DELETE FROM " + config["DB"]["DB"] + "." + config["YANDEX_METRIKA"]["TABLE_VISITS_GOALS"] + " WHERE `ym:s:goalDateTime`>='" + yesterday_1 + "'"}, headers={'Content-Type':'application/octet-stream'}, verify=False)
# добавляем новые данные, цели
                    csv_file = data.to_csv(index=False).encode('utf-8')
                    requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                        params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_METRIKA"]["TABLE_VISITS_GOALS"] + ' FORMAT CSV'},
                        headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
        data2clean = False
        visits_total += len(data)
        goals_total += len(goals)
# удаляем обработанный запрос из API
        api.clean(requestId=request_id).post()
        print (COUNTER_ID + " | " + yesterday + ": " + str(visits_total) + "/" + str(goals_total))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
    connection.commit()
    connection.close()
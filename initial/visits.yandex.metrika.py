# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import requests
import time
from tapi_yandex_metrika import YandexMetrikaLogsapi
import numpy as np

# импорт настроек
import configparser
config = configparser.ConfigParser()
config.read("../settings.ini")

# импорт библиотек для работы с БД
if config["DB"]["TYPE"] == "MYSQL":
    import mysql.connector as db_connector
elif config["DB"]["TYPE"] == "POSTGRESQL":
    import psycopg2 as db_connector
elif config["DB"]["TYPE"] == "MARIADB":
    import mariadb as db_connector

# создание подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB"]:
    connection = db_connector.connect(
      host=config["DB"]["HOST"],
      user=config["DB"]["USER"],
      password=config["DB"]["PASSWORD"],
      database=config["DB"]["DB"]
    )
    cursor = connection.cursor()

# создаем таблицу для данных при наличии каких-либо данных
table_not_created = True
api = YandexMetrikaLogsapi(access_token=config["YANDEX_METRIKA"]["ACCESS_TOKEN"], default_url_params={'counterId': config["YANDEX_METRIKA"]["COUNTER_ID"]})
# выгружаем данные за 5 лет по годам
for period in range(int(config["YANDEX_METRIKA"]["PERIODS"]), 0, -1):
# Создание запроса на выгрузку данных (ежегодно)
    date_since = (date.today() - timedelta(days=int(config["YANDEX_METRIKA"]["DELTA"])*(period-1)+1)).strftime('%Y-%m-%d')
    date_until = (date.today() - timedelta(days=int(config["YANDEX_METRIKA"]["DELTA"])*period)).strftime('%Y-%m-%d')
    params = {
        "fields": "ym:s:visitID,ym:s:dateTime,ym:s:isNewUser,ym:s:startURL,ym:s:endURL,ym:s:pageViews,ym:s:visitDuration,ym:s:ipAddress,ym:s:regionCountry,ym:s:regionCity,ym:s:regionCountryID,ym:s:regionCityID,ym:s:clientID,ym:s:networkType,ym:s:goalsID,ym:s:referer,ym:s:from,ym:s:lastTrafficSource,ym:s:lastAdvEngine,ym:s:lastReferalSource,ym:s:lastSearchEngineRoot,ym:s:lastSearchEngine,ym:s:lastSocialNetwork,ym:s:lastSocialNetworkProfile,ym:s:lastDirectClickOrder,ym:s:lastDirectPlatformType,ym:s:lastDirectPlatform,ym:s:lastUTMCampaign,ym:s:lastUTMContent,ym:s:lastUTMMedium,ym:s:lastUTMSource,ym:s:lastUTMTerm,ym:s:lastRecommendationSystem,ym:s:lastGCLID,ym:s:lastMessenger,ym:s:browser,ym:s:browserLanguage,ym:s:browserCountry,ym:s:deviceCategory,ym:s:mobilePhone,ym:s:mobilePhoneModel,ym:s:operatingSystemRoot,ym:s:operatingSystem,ym:s:browserMajorVersion,ym:s:browserMinorVersion,ym:s:browserEngine,ym:s:browserEngineVersion1,ym:s:browserEngineVersion2,ym:s:browserEngineVersion3,ym:s:browserEngineVersion4",
        "source": "visits",
        "date1": date_until,
        "date2": date_since
    }
# отправляем запрос API
    result = api.create().post(params=params)
# получаем ID в очереди
    request_id = result["log_request"]["request_id"]
    info = api.info(requestId=request_id).get()
# ждем выполнения запроса
    while info["log_request"]["status"] != "processed":
        time.sleep(10)
        info = api.info(requestId=request_id).get()
# обрабатываем результат запроса
    for p in info["log_request"]["parts"]:
        part = api.download(requestId=request_id, partNumber=p["part_number"]).get()
# формируем датафрейм по отдельности из каждой части
        data = pd.DataFrame(part().to_values(), columns=part.columns)
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["ym:s:isNewUser", "ym:s:pageViews", "ym:s:regionCountryID", "ym:s:regionCityID", "ym:s:visitID", "ym:s:browserMajorVersion", "ym:s:browserMinorVersion", "ym:s:browserEngineVersion1", "ym:s:browserEngineVersion2", "ym:s:browserEngineVersion3", "ym:s:browserEngineVersion4"]:
                data[col] = data[col].fillna('').replace('', 0).astype(np.uint64)
# приведение вещественных чисел
            elif col in ["ym:s:visitDuration"]:
                data[col] = data[col].fillna(0.0).astype(float)
# приведение списков
            elif col in ["ym:s:goalsID"]:
                data[col] = data[col].apply(lambda x:'#'.join(x)).fillna('')
# приведение дат
            elif col in ["ym:s:dateTime"]:
                data[col] = pd.to_datetime(data[col].fillna("2000-01-01 00:00:00").apply(lambda x: dt.strptime(x, "%Y-%m-%d %H:%M:%S")))
# приведение строк
            else:
                data[col] = data[col].fillna('')
        if len(data):
# добавляем метку времени
            data["ts"] = pd.DatetimeIndex(data["ym:s:dateTime"]).asi8
# создаем таблицу в первый раз
            if table_not_created:
                if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB"]:
                    cursor.execute((pd.io.sql.get_schema(data, config["YANDEX_METRIKA"]["TABLE_VISITS"])).replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "))
                    connection.commit()
                elif config["DB"]["TYPE"] == "CLICKHOUSE":
                    requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                        params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["YANDEX_METRIKA"]["TABLE_VISITS"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".")})
                table_not_created = False
            if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB"]:
                data.to_sql(name=config["YANDEX_METRIKA"]["TABLE_VISITS"], con=connection, if_exists='append')
                connection.commit()
            elif config["DB"]["TYPE"] == "CLICKHOUSE":
                csv_file = data.to_csv().encode('utf-8')
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                    params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_METRIKA"]["TABLE_VISITS"] + ' FORMAT CSV'},
                    headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
# удаляем обработанный запрос из API
    api.clean(requestId=request_id).post()
    print (date_since + "=>" + date_until + ": " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB"]:
    cursor.close()
    connection.close()
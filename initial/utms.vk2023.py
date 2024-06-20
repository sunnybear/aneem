# Скрипт для получения +- полного списка кампаний и UTM меток объявлений кабинетов вКонтакте после 2023 года
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * VK2023.ACCESS_TOKEN - Access Token (бессрочный, агентский) как альтернатива клиентскому набору Client Secret/Client Id/Refresh Token
# * VK2023.CLIENT_SECRET - Client Secret из настроек аккаунта
# * VK2023.CLIENT_ID - Client Id из настроек аккаунта
# * VK2023.REFRESH_TOKEN - Refresh Token (получается после запроса ACCESS TOKEN в API ВК), используется для обновления клиентского ACCESS TOKEN
# * VK2023.TABLE_UTMS - имя результирующей таблицы для UTM меток

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
import time
from sqlalchemy import create_engine, text
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from io import StringIO
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

# перебор всех токенов при необходимости
if config["VK_2023"]["ACCESS_TOKEN"]:
    TOKENS = config["VK_2023"]["ACCESS_TOKEN"].split(",")
else:
    TOKENS = config["VK_2023"]["REFRESH_TOKEN"].split(",")

for i, TOKEN in enumerate(TOKENS):
# обновляем токен ВК, если требуется
    if config["VK_2023"]["ACCESS_TOKEN"]:
        vk2023_access_token = TOKEN
    else:
        r_refresh = requests.post('https://ads.vk.com/api/v2/oauth2/token.json', data={
            'grant_type': 'refresh_token',
            'refresh_token': TOKEN,
            'client_id': config["VK_2023"]["CLIENT_ID"].split(",")[i],
            'client_secret': config["VK_2023"]["CLIENT_SECRET"].split(",")[i]
        }).json()
        vk2023_access_token = r_refresh['access_token']

# Задержка в 1 секунду для избежания превышения лимитов по запросам
    time.sleep(1)
# Создание запроса на выгрузку пакетов групп объявлений
    r_packages = requests.get("https://ads.vk.com/api/v2/packages.json", headers = {'Authorization': 'Bearer ' + vk2023_access_token}).json()
# формируем список пакетов с UTM
    packages = {}
    if "items" in r_packages.keys():
        for k in r_packages["items"]:
            if k["utm"]:
# преобразуем строку адреса из URL Decoded формата
                href = requests.utils.unquote(k["utm"]).replace("+", " ")
                utm_values = []
                for utm in ['utm_source', 'utm_medium', 'utm_campaign']:
                    if href.find(utm) > -1:
                        utm_start = href.find(utm)
                        utm_end = href[href.find(utm):].find('&')
                        if utm_end == -1:
                            utm_end = len(href)
                        else:
                            utm_end += utm_start
                        utm_values.append(href[utm_start + len(utm) + 1:utm_end])
                    else:
                        utm_values.append('')
                packages[k["id"]] = [href, utm_values[0], utm_values[1], utm_values[2]]
            else:
                packages[k["id"]] = ['', 'cpc', 'vk', '']
# Создание запроса на выгрузку объявлений
    r_ads = requests.get("https://ads.vk.com/api/v2/ad_plans.json?fields=id,package_id,name", headers = {'Authorization': 'Bearer ' + vk2023_access_token}).json()
# формируем список обхявлений с UTM из пакетов
    items = []
    if "items" in r_ads.keys():
        for k in r_ads["items"]:
            if k["package_id"] in packages.keys():
                if packages[k["package_id"]][3] == '':
                    utm_campaign = k["id"]
                else:
                    utm_campaign = packages[k["package_id"]][3].replace('{{campaign_id}}', str(k["id"])).replace('{{campaign_name}}', k["name"])
                items.append([k["id"], k["name"], packages[k["package_id"]][0], packages[k["package_id"]][1], packages[k["package_id"]][2], utm_campaign])
# формируем датафрейм из полученных меток
    data = pd.DataFrame(items, columns=["CampaignId", "CampaignName", "CampaignHref", "UTMSource", "UTMMedium", "UTMCampaign"])
# базовый процесс очистки: приведение к нужным типам
    for col in data.columns:
# приведение целых чисел
        if col in ["CampaignId"]:
            data[col] = data[col].fillna(0).astype(np.int64)
# приведение строк
        else:
            data[col] = data[col].fillna('')
    if len(data):
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                connection.execute(text("DROP TABLE IF EXISTS " + config["VK_2023"]["TABLE_UTMS"]))
                connection.commit()
            elif config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data.reset_index(), config["VK_2023"]["TABLE_UTMS"]) + "  ENGINE=MergeTree ORDER BY (`index`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["VK_2023"]["TABLE_UTMS"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.reset_index().to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["VK_2023"]["TABLE_UTMS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print ("TOKEN" + str(i) + " | " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["VK_2023"]["TABLE_UTMS"] + " ADD INDEX campaignidx (`CampaignId`)"))
    connection.execute(text("ALTER TABLE " + config["VK_2023"]["TABLE_UTMS"] + " ADD INDEX utmmediumidx (`UTMMedium`)"))
    connection.execute(text("ALTER TABLE " + config["VK_2023"]["TABLE_UTMS"] + " ADD INDEX utmsourceidx (`UTMSource`)"))
    connection.execute(text("ALTER TABLE " + config["VK_2023"]["TABLE_UTMS"] + " ADD INDEX utmcampaignidx (`UTMCampaign`)"))
    connection.commit()
    connection.close()
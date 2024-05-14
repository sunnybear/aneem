# Скрипт для получения полного списка кампаний и UTM меток объявлений кабинета Яндекс.Директа
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * YANDEX_DIRECT.ACCESS_TOKEN - Access Token для приложения, имеющего доступ к статистике нужного сайта (или несколько - через запятую, порядок как у логина)
# * YANDEX_DIRECT.LOGIN - Логин аккаунта Яндекс.Директа, для которого разрешен доступ к статистике (или несколько - через запятую в том же порядке)
# * YANDEX_DIRECT.TABLE_UTMS - имя результирующей таблицы для UTM меток

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
from tapi_yandex_direct import YandexDirect
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
# перебираем все доступы к рекламным кабинетам
for i_credentials, TOKEN in enumerate(config["YANDEX_DIRECT"]["ACCESS_TOKEN"].split(",")):
    TOKEN = TOKEN.strip()
    LOGIN = config["YANDEX_DIRECT"]["LOGIN"].split(",")[i_credentials].strip()
# создание подключения к API Яндекс.Директ
    client = YandexDirect(
        access_token = TOKEN,
        login = LOGIN,
        is_sandbox = False,
        retry_if_not_enough_units = False,
        language = "ru",
        retry_if_exceeded_limit = True,
        retries_if_server_error = 5,
        processing_mode = "offline",
        wait_report = True,
        return_money_in_micros = False,
        skip_report_header = True,
        skip_column_header = False,
        skip_report_summary = True
    )

# получаем все кампании из аккаунта
    body = {
        "method": "get",
        "params": {"SelectionCriteria": {}, "FieldNames": ["Id", "Name"]}
    }
    campaigns = client.campaigns().post(data=body)
    campaigns = pd.DataFrame(campaigns().items())

# исходный список кампаний с UTM метками
    items = []
# получаем все объявления для данной кампании, интересует только ссылка в объявлении
    for cid in campaigns["Id"].values:
        body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {"CampaignIds": [int(cid)]},
                "FieldNames": ["Id"],
                "TextAdFieldNames": ["Href"],
                "MobileAppAdFieldNames": ["TrackingUrl"],
                "TextImageAdFieldNames": ["Href"],
                "MobileAppImageAdFieldNames": ["TrackingUrl"],
                "TextAdBuilderAdFieldNames": ["Href"],
                "MobileAppAdBuilderAdFieldNames": ["TrackingUrl"],
                "MobileAppCpcVideoAdBuilderAdFieldNames": ["TrackingUrl"],
                "CpcVideoAdBuilderAdFieldNames": ["Href"],
                "CpmBannerAdBuilderAdFieldNames": ["Href"],
                "CpmVideoAdBuilderAdFieldNames": ["Href"]
            },
        }
        ads = client.ads().post(data=body)
# перебираем все объявления, ищем первое с размеченной ссылкой
        for ad in ads().extract():
            href = ''
            utm_values = []
# набор типов объявлений, где ищем Href
            for f in ["TextAd", "TextImageAd", "TextAdBuilderAd", "CpcVideoAdBuilderAd", "CpmBannerAdBuilderAd", "CpmVideoAdBuilderAd"]:
                if ad.get(f) is not None:
                    href = ad[f]["Href"]
# набор типов объявлений, где ищем TrackingUrl
            for f in ["MobileAppAd", "MobileAppImageAd", "MobileAppAdBuilderAd", "MobileAppCpcVideoAdBuilderAd"]:
                if ad.get(f) is not None:
                    href = ad[f]["TrackingUrl"]
            if href != '':
# если ссылка найдена - извлекаем из нее метки
                for utm in ['utm_medium', 'utm_source', 'utm_campaign']:
                    if href.find(utm) > -1:
                        utm_start = href.find(utm)
                        utm_end = href[href.find(utm):].find('&')
                        if utm_end == -1:
                            utm_end = len(href)
                        else:
                            utm_end += utm_start
# подменяем в метках переменные Яндекс.Директа
                        utm_values.append(href[utm_start + len(utm) + 1:utm_end].replace('{campaign_id}', str(cid)).replace('{campaign_name}', campaigns.loc[campaigns["Id"]==cid]["Name"].values[0]))
                    else:
                        utm_values.append('')
# останавливаемся, как только нашли полный набор меток
            if len(utm_values) != 0 and utm_values[0] != '' and utm_values[1] != '' and utm_values[2] != '':
                break
# метки "по умолчанию" для кампании, финально применятся только после перебора всех объявлений
        if len(utm_values) == 0 or utm_values[0] == utm_values[1] == utm_values[2] == '':
            utm_values = ['cpc', 'yandex', str(cid)]
        items.append([LOGIN, cid, href, utm_values[0], utm_values[1], utm_values[2]])

# формируем датафрейм из полученных меток
    data = pd.DataFrame(items, columns=["ClientLogin", "CampaignId", "CampaignHref", "UTMSource", "UTMMedium", "UTMCampaign"])
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
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data.reset_index(), config["YANDEX_DIRECT"]["TABLE_UTMS"]) + "  ENGINE=MergeTree ORDER BY (`index`)").replace("CREATE TABLE ", "CREATE OR REPLACE TABLE " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["YANDEX_DIRECT"]["TABLE_UTMS"], con=engine, if_exists='replace', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.reset_index().to_csv().encode('utf-8')
            requests.post('https://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':8443/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["YANDEX_DIRECT"]["TABLE_UTMS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
    print (LOGIN + " | " + str(len(data)))

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["YANDEX_DIRECT"]["TABLE_UTMS"] + " ADD INDEX campaignid (`CampaignId`)"))
    connection.execute(text("ALTER TABLE " + config["YANDEX_DIRECT"]["TABLE_UTMS"] + " ADD INDEX source (`UTMSource`)"))
    connection.execute(text("ALTER TABLE " + config["YANDEX_DIRECT"]["TABLE_UTMS"] + " ADD INDEX medium (`UTMMedium`)"))
    connection.execute(text("ALTER TABLE " + config["YANDEX_DIRECT"]["TABLE_UTMS"] + " ADD INDEX campaign (`UTMCampaign`)"))
    connection.commit()
    connection.close()
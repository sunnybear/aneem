# Скрипт для ежедневного копирования выгрузки 1С (Денвик) в случае наличия данных (отсутствия ошибок)
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)

# requirements.txt:
# requests
# sqlalchemy

# timeout: 300
# memory: 128

# импорт общих библиотек
import os
import requests
from sqlalchemy import create_engine, text
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# Скрытие предупреждения Unverified HTTPS request
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def handler(event, context):
    auth = {
        'X-ClickHouse-User': os.getenv('DB_USER'),
        'X-ClickHouse-Key': context.token["access_token"]
    }
    auth_post = auth.copy()
    auth_post['Content-Type'] = 'application/octet-stream'
    cacert = '/etc/ssl/certs/ca-certificates.crt'
    ret = []

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

# словарь таблиц, которые необходимо копировать. Формат: таблица-источник => таблица-преемник
    tables = {
        "raw_1c_Продажи_": "raw_1c_Продажи"
    }
    for TABLE_SOURCE in tables.keys():
        TABLE_DESTINATION = tables[TABLE_SOURCE]
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            table_total = int(pd.read_sql("SELECT COUNT(*) AS c FROM `" + TABLE_SOURCE + "`", connection)['c'].values[0])
# если пришли новые данные - удаляем старые и копируем новые вместо старых
            if table_total > 0:
                try:
                    connection.execute(text("DROP TABLE IF EXISTS `" + TABLE_DESTINATION + "`"))
                    connection.commit()
                    connection.execute(text("CREATE TABLE `" + TABLE_DESTINATION + "` AS `" + TABLE_SOURCE + "`"))
                    connection.commit()
                    connection.execute(text("INSERT INTO `" + TABLE_DESTINATION + "` SELECT * FROM `" + TABLE_SOURCE + "`"))
                    connection.commit()
                    ret.append(TABLE_DESTINATION)
                except Exception as E:
                    print (E)
                    connection.rollback()
        elif os.getenv('DB_TYPE') == "CLICKHOUSE":
            table_total = int(requests.get("https://" + os.getenv('DB_HOST') + ":8443/?database=" + os.getenv('DB_DB') + "&query=SELECT COUNT(*) FROM " + os.getenv('DB_PREFIX') + ".\"" + TABLE_SOURCE + "\" WHERE 1=1",
                headers=auth, verify=cacert).text)
            if table_total > 0:
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": "CREATE OR REPLACE TABLE " + os.getenv('DB_PREFIX') + ".\"" + TABLE_DESTINATION + "\" AS " + os.getenv('DB_PREFIX') + ".\"" + TABLE_SOURCE + "\""})
                requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth, verify=cacert,
                    params={"database": os.getenv('DB_DB'), "query": "INSERT INTO " + os.getenv('DB_PREFIX') + ".\"" + TABLE_DESTINATION + "\" SELECT * FROM " + os.getenv('DB_PREFIX') + ".\"" + TABLE_SOURCE + "\""})
                ret.append(TABLE_DESTINATION)
    return {
        'statusCode': 200,
        'body': "Updated: " + ','.join(ret)
    }
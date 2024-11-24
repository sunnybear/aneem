# Скрипт для проверки факта ежедневного получения данных из источников
# Для уведомлений в Телеграм необходимо создать бота: получить ключ бота через @BotFather
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * TELEGRAM_BOT_TOKEN - ключ Telegram бота
# * TELEGRAM_BOT_CHATIDS - чаты, в которые отправлять уведомления о проблемах с данными, через запятую. Например: -1001234567890
# * TELEGRAM_BOT_MESSAGE - сообщение, которое отправлять. Например: Проблема с данными в таблице {table} за {date}

# requirements.txt:
# pandas
# requests
# datetime
# sqlalchemy

# timeout: 300
# memory: 256

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import requests
import time
import os
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
    yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    threedaysago = (date.today() - timedelta(days=3)).strftime('%Y-%m-%d')

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

# формат: название таблицы => (поле с датой, имя таблицы в сообщении, дней проверки)
    tables = {
        'raw_ym_visits': ('ym:s:dateTime', 'Яндекс.Метрика: визиты', 1)
#        ,'raw_yd_costs': ('Date', 'Яндекс.Директ: расходы', 1)
#        ,'raw_ym_costs': ('ym:ev:date', 'Яндекс.Метрика: расходы', 1)
#        ,'raw_bx_crm_lead': ('DATE_CREATE', 'Битрикс24: лиды', 3)
#        ,'raw_bx_crm_deal': ('DATE_CREATE', 'Битрикс24: сделки', 3)
#        ,'raw_yw_shows_daily': ('Date', 'Yandex.Wordstat: показы', 1)
    }
    alerts = 0

# проверяем данные во всех таблицах
    for TABLE in tables.keys():
        TABLE_FIELD = tables[TABLE][0]
        if tables[TABLE][2] == 3:
            DATE_DELTA = threedaysago
        else:
            DATE_DELTA = yesterday
# проверяем вчерашние данные
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            try:
                table_total = int(pd.read_sql("SELECT COUNT(*) AS c FROM " + TABLE + " WHERE `" + TABLE_FIELD + "`>='" + DATE_DELTA + "'", connection)['c'].values[0])
                connection.commit()
            except Exception as E:
                print (E)
                connection.rollback()
        elif os.getenv('DB_TYPE') == "CLICKHOUSE":
            table_total = int(requests.get('https://' + os.getenv('DB_HOST') + ':8443', headers=auth, verify=cacert,
                params={"database": os.getenv('DB_DB'), "query": "SELECT COUNT(*) FROM " + os.getenv('DB_PREFIX') + "." + TABLE + " WHERE `" + TABLE_FIELD + "`>='" + DATE_DELTA + "'"}).text)
# данных нет - отправляем уведомление во все чаты
        if table_total == 0:
            message = os.getenv('TELEGRAM_BOT_MESSAGE').replace('{table}', tables[TABLE][1]).replace('{date}', yesterday)
            for CHAT_ID in os.getenv('TELEGRAM_BOT_CHATIDS').split(','):
                requests.get("https://api.telegram.org/bot" + os.getenv('TELEGRAM_BOT_TOKEN') + "/sendMessage?chat_id=" + CHAT_ID.strip() + "&text=" + message)
            alerts += 1
    if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
        connection.close()

    return {
        'statusCode': 200,
        'body': "SentAlerts: " + str(alerts)
    }
# Скрипт для ежедневного обновления материализованных представлений Clickhouse
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
import requests
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
    ret = []

# список Materialized Views (ground table) для обновления в порядке зависимостей
    mvs = ["dict_bxdealid_phone", "dict_bxleadid_phone", "dict_ctphone_attribution_lndc", "dict_ctphone_yclid", "dict_yainstallationid_phone", "dict_yainstallationid_phone_hash", "dict_yainstallationid_yclid", "dict_yclid_attribution_lndc", "mart_mkt_bx_crm_lead", "mart_mkt_bx_crm_deal", "mart_mkt_bx_deals_app", "mart_mkt_bx_leads_app"]
    for mv in mvs:
        req_sql_view = requests.get('https://' + os.getenv('DB_HOST') + ':8443', headers=auth, verify=cacert,
            params={"database": os.getenv('DB_DB'), "query": "SHOW CREATE VIEW " + os.getenv('DB_PREFIX') + "." + mv + "_mv"})
        if req_sql_view.status_code == 200:
            req_sql = req_sql_view.text[req_sql_view.text.find("SELECT"):].replace("\\n"," ").replace("\\","")
# очистка таблицы
            requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                params={"database": os.getenv('DB_DB'), "query": "TRUNCATE TABLE " + os.getenv('DB_PREFIX') + "." + mv})
# заполнение таблицы заново
            requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth_post, verify=cacert,
                params={"database": os.getenv('DB_DB'), "query": "INSERT INTO " + os.getenv('DB_PREFIX') + "." + mv + " " + req_sql})
            ret.append(mv)
    return {
        'statusCode': 200,
        'body': "Updated: " + ','.join(ret)
    }
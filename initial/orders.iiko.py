# Скрипт для первоначального получения всех данных по заказам из Iiko
# Необходимо в settings.ini указать
# * DB.TYPE - тип базы данных (куда выгружать данные)
# * DB.HOST - адрес (хост) базы данных
# * DB.PORT - порт хоста базы данных (если отличается от стандартного)
# * DB.USER - пользователь базы данных
# * DB.PASSWORD - пароль к базе данных
# * DB.DB - имя базы данных
# * IIKO.API_ENDPOINT - точка доступа API
# * IIKO.ACCESS_TOKEN_LOGIN - логин для получения Access Token
# * IIKO.ACCESS_TOKEN_PASS - логин для получения Access Token
# * IIKO.TABLE_ORDERS - имя результирующей таблицы для заказов

# импорт общих библиотек
from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import requests
import time
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
if config["DB"]["PORT"] != '':
    DB_PORT = ':' + config["DB"]["PORT"]
else:
    DB_PORT = ''
if config["DB"]["TYPE"] == "MYSQL":
    engine = create_engine('mysql+mysqldb://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "POSTGRESQL":
    engine = create_engine('postgresql+psycopg2://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"] + '?client_encoding=utf8')
elif config["DB"]["TYPE"] == "MARIADB":
    engine = create_engine('mariadb+mysqldb://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"] + '?charset=utf8')
elif config["DB"]["TYPE"] == "ORACLE":
    engine = create_engine('oracle+pyodbc://' + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + DB_PORT + '/' + config["DB"]["DB"])
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
token_timestamp = time.time()
TOKEN = ''

# выгружаем данные по заказам периодам по 30 дней
for period in range(int(config['IIKO']['PERIODS']), 0, -1):
    delta = int(config['IIKO']['DELTA'])
    date_since = (date.today() - timedelta(days=period*delta)).strftime('%Y-%m-%d')
    date_until = (date.today() - timedelta(days=(period-1)*delta)).strftime('%Y-%m-%d')

# получение временного токена
    if TOKEN == '' or time.time() - token_timestamp > 900:
        result_token = requests.get(config['IIKO']['API_ENDPOINT'] + '/resto/api/auth?login=' + config['IIKO']['ACCESS_TOKEN_LOGIN'] + '&pass=' + config['IIKO']['ACCESS_TOKEN_PASS'])
        TOKEN = result_token.text
        token_timestamp = time.time()

# отправка основного запроса
    result = requests.post(config['IIKO']['API_ENDPOINT'] + '/resto/api/v2/reports/olap',
        headers = {'Cookie': 'key=' + TOKEN, 'Accept-Type': 'application/json'},
        json = {"reportType":"SALES",
            "groupByColFields": ["Cashier.Id", "Cashier", "CashRegisterName.Number", "CashRegisterName", "CloseTime", "CloseTime.Minutes15", "Comment", "CookingPlace", "Currencies.CurrencyRate", "Currencies.Currency", "DayOfWeekOpen", "Department.Id", "Department", "DiscountPercent", "DishCategory.Id", "DishCode", "DishCategory", "DishGroup.Id", "DishGroup.Num", "DishGroup", "DishGroup.Hierarchy", "DishGroup.SecondParent", "DishGroup.ThirdParent", "DishGroup.TopParent", "DishId", "DishName", "DishServicePrintTime", "DishType", "FiscalChequeNumber", "GuestNum", "HourClose", "HourOpen", "JurName", "Mounth", "NonCashPaymentType", "NonCashPaymentType.DocumentType", "OpenDate.Typed", "OpenTime", "OpenTime.Minutes15", "OperationType", "OrderDiscount.GuestCard", "OrderDiscount.Type", "OrderDiscount.Type.IDs", "OrderNum", "OrderServiceType", "OrderTime.OrderLength", "OrderTime.PrechequeLength", "OrderWaiter.Id", "OrderWaiter.Name", "PayTypes.Combo", "PayTypes.IsPrintCheque", "PrechequeTime", "RemovalType", "RestaurantSection.Id", "RestaurantSection", "SessionNum", "SoldWithDish", "SoldWithDish.Id", "Store.Id", "Store.Name", "Storned", "TableNum", "UniqOrderId.Id", "WaiterName.ID", "WaiterName", "WeekInMonthOpen", "WeekInYearOpen", "YearOpen"],
            "aggregateFields": ["Cooking.GuestWaitTime.Avg", "DiscountSum", "discountWithoutVAT", "DishAmountInt.PerOrder", "DishDiscountSumInt", "DishDiscountSumInt.averageByGuest", "DishReturnSum.withoutVAT", "fullSum", "OrderItems", "ProductCostBase.OneItem", "ProductCostBase.Percent", "ProductCostBase.PercentWithoutVAT", "ProductCostBase.ProductCost", "ProductCostBase.Profit", "UniqOrderId", "UniqOrderId.OrdersCount"],
            "filters": {
                "OpenDate.Typed": {"filterType":"DateRange", "periodType":"CUSTOM", "from": date_since, "to": date_until},
                "OpenTime": {"filterType":"DateRange", "periodType":"CUSTOM", "from": date_since, "to": date_until, "includeLow": True, "includeHigh": True},
                "DeletedWithWriteoff": {"filterType": "ExcludeValues", "values": ["DELETED_WITH_WRITEOFF","DELETED_WITHOUT_WRITEOFF"]},
                "OrderDeleted": {"filterType": "IncludeValues", "values": ["NOT_DELETED"]}
            }
        })

    data = pd.DataFrame()
    if len(result.text) > 500 and 'data' in result.json():
# формируем датафрейм из ответа API
        data = pd.DataFrame(result.json()['data'])
# базовый процесс очистки: приведение к нужным типам
        for col in data.columns:
# приведение целых чисел
            if col in ["CashRegisterName.Number", "DishGroup.Num", "FiscalChequeNumber", "GuestNum", "HourClose", "HourOpen", "OrderNum", "OrderTime.OrderLength", "SessionNum", "TableNum", "UniqOrderId", "WeekInMonthOpen", "WeekInYearOpen", "YearOpen", "DishAmountInt.PerOrder", "DishDiscountSumInt", "DishDiscountSumInt.averageByGuest", "OrderItems", "UniqOrderId.OrdersCount"]:
                data[col] = data[col].fillna(0).replace('', 0).astype(np.int64)
# приведение вещественных чисел
            elif col in ["Cooking.GuestWaitTime.Avg", "DiscountSum", "discountWithoutVAT", "DishReturnSum.withoutVAT", "fullSum", "ProductCostBase.OneItem", "ProductCostBase.Percent", "ProductCostBase.PercentWithoutVAT", "ProductCostBase.ProductCost", "ProductCostBase.Profit"]:
                data[col] = data[col].fillna(0.0).replace('', 0.0).astype(float)
# приведение дат
            elif col in ["OpenDate.Typed", "OpenTime"]:
                data[col] = pd.to_datetime(data[col], format='ISO8601')
            elif col in ["CloseTime", "PrechequeTime"]:
                data[col] = pd.to_datetime(data[col].str.replace(r'\..*', '', regex=True), format='ISO8601')
# приведение строк
            else:
                data[col] = data[col].fillna('')

    if len(data):
        data["ts"] = pd.DatetimeIndex(data["OpenDate.Typed"]).asi8
# поддержка TCP HTTP для Clickhouse
        if config["DB"]["PORT"] != '8443':
            CLICKHOUSE_PROTO = 'http://'
            CLICKHOUSE_PORT = config["DB"]["PORT"]
        else:
            CLICKHOUSE_PROTO = 'https://'
            CLICKHOUSE_PORT = config["DB"]["PORT"]
# создаем таблицу в первый раз
        if table_not_created:
            if config["DB"]["TYPE"] == "CLICKHOUSE":
                requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/', verify=False,
                    params={"database": config["DB"]["DB"], "query": (pd.io.sql.get_schema(data, config["IIKO"]["TABLE_ORDERS"]) + "  ENGINE=MergeTree ORDER BY (`ts`)").replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS " + config["DB"]["DB"] + ".").replace("INTEGER", "Int64")})
            table_not_created = False
        if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# обработка ошибок при добавлении данных
            try:
                data.to_sql(name=config["IIKO"]["TABLE_ORDERS"], con=engine, if_exists='append', chunksize=100)
            except Exception as E:
                print (E)
                connection.rollback()
        elif config["DB"]["TYPE"] == "CLICKHOUSE":
            csv_file = data.to_csv(index=False).encode('utf-8')
            requests.post(CLICKHOUSE_PROTO + config["DB"]["USER"] + ':' + config["DB"]["PASSWORD"] + '@' + config["DB"]["HOST"] + ':' + CLICKHOUSE_PORT + '/',
                params={"database": config["DB"]["DB"], "query": 'INSERT INTO ' + config["DB"]["DB"] + '.' + config["IIKO"]["TABLE_ORDERS"] + ' FORMAT CSV'},
                headers={'Content-Type':'application/octet-stream'}, data=csv_file, stream=True, verify=False)
        print (date_since, "=>", date_until, ":", len(data))
    else:
        print (date_since, "=>", date_until, ":", result.text[:500])

# закрытие подключения к БД
if config["DB"]["TYPE"] in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
# добавление индексов
    connection.execute(text("ALTER TABLE " + config["IIKO"]["TABLE_ORDERS"] + " ADD INDEX dateidx (`date`)"))
    connection.commit()
    connection.close()
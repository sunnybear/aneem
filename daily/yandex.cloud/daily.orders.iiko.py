# Скрипт для первоначального получения всех данных по заказам из Iiko
# Необходимо в переменных окружения указать
# * DB_TYPE - тип базы данных (куда выгружать данные)
# * DB_HOST - адрес (хост) базы данных
# * DB_USER - пользователь базы данных
# * DB_PASSWORD - пароль к базе данных (если требуется)
# * DB_DB - имя базы данных
# * DB_PREFIX - префикс базы данных (может отличаться от имени при облачном подключении)
# * IIKO_API_ENDPOINT - точка доступа API
# * IIKO_ACCESS_TOKEN_LOGIN - логин для получения Access Token
# * IIKO_ACCESS_TOKEN_PASS - логин для получения Access Token
# * IIKO_TABLE_ORDERS - имя результирующей таблицы для заказов

# requirements.txt:
# pandas
# numpy
# requests
# datetime
# sqlalchemy

# timeout: 300
# memory: 8096

from datetime import datetime as dt
from datetime import date, timedelta
import pandas as pd
import numpy as np
import os
import requests
from sqlalchemy import create_engine, text

def handler(event, context):
    auth = {
        'X-ClickHouse-User': os.getenv('DB_USER'),
        'X-ClickHouse-Key': context.token["access_token"]
    }
    auth_post = auth.copy()
    auth_post['Content-Type'] = 'application/octet-stream'
    cacert = '/etc/ssl/certs/ca-certificates.crt'
# если данных слишком много - ограничить периодом ровно сутками
    date_since = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    date_until = (date.today() + timedelta(days=1)).strftime('%Y-%m-%d')

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

# нужно ли удалить все данные за вчера
    data_not_cleaned = True
# получение временного токена
    result_token = requests.get(config['IIKO']['API_ENDPOINT'] + '/resto/api/auth?login=' + config['IIKO']['ACCESS_TOKEN_LOGIN'] + '&pass=' + config['IIKO']['ACCESS_TOKEN_PASS'])
    TOKEN = result_token.text

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
        data = pd.DataFrame(result.json())
# очистка памяти
        del result
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
            if data_not_cleaned:
# удаление старых данных
                if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                    try:
                        connection.execute(text("DELETE FROM " + os.getenv('IIKO_TABLE_ORDERS') + " WHERE `OpenDate.Typed`>='" + date_since + "'"))
                        connection.commit()
                    except Exception as E:
                        print (E)
                        connection.rollback()
                elif os.getenv('DB_TYPE') == "CLICKHOUSE":
# удаление старых данных
                    requests.post('https://' + os.getenv('DB_HOST') + ':8443', headers=auth, verify=cacert,
                        params={"database": os.getenv('DB_DB'), "query": "DELETE FROM " + os.getenv('DB_PREFIX') + "." + os.getenv('IIKO_TABLE_ORDERS') + " WHERE `OpenDate.Typed`>='" + date_since + "'"})
            data_not_cleaned = False
# добавление новых данных
            if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
                try:
                    data.to_sql(name=os.getenv('IIKO_TABLE_ORDERS'), con=engine, if_exists='append', chunksize=100)
                    connection.commit()
                except Exception as E:
                    print (E)
                    connection.rollback()
            elif os.getenv('DB_TYPE') == "CLICKHOUSE":
                csv_file = data.to_csv().encode('utf-8')
                requests.post('https://' + os.getenv('DB_HOST') + ':8443/?database=' + os.getenv('DB_DB') + '&query=INSERT INTO ' + os.getenv('DB_PREFIX') + '.' + os.getenv('IIKO_TABLE_ORDERS') + ' FORMAT CSV',
                    headers=auth_post, data=csv_file, stream=True)
        if os.getenv('DB_TYPE') in ["MYSQL", "POSTGRESQL", "MARIADB", "ORACLE", "SQLITE"]:
            connection.close()

    return {
        'statusCode': 200,
        'body': "LoadedOrders: " + str(len(data))
    }
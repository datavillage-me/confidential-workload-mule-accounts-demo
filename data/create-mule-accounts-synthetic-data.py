import duckdb
import random
import os
import datetime
import numpy as np 

from duckdb.typing import *
from faker import Faker

# {
#     "mule-accounts": {
#         "type": "table",
#         "fields": {
#             "account_uuid": {
#                 "type": "varchar",
#                 "required": true,
#                 "pii": true,
#                 "classification": "sensitive"
#             },
#             "account_number": {
#                 "type": "varchar",
#                 "required": true,
#                 "pii": true,
#                 "classification": "sensitive"
#             },
#             "account_format": {
#                 "type": "varchar",
#                 "required": true,
#             },
#             "bank_id": {
#                 "type": "varchar",
#                 "required": true,
#                 "pii": true,
#                 "classification": "sensitive"
#             },
#             "date_added": {
#                 "type": "date",
#                 "required": true,
#             },
#             "critical_account": {
#                 "type": "varchar",
#                 "required": true,
#             }
#         }
#     }
# }




# Locales for Europe, the UK, and North America

locales=["en_GB","de_DE","en_GB"]


def random_uuid(n):
    fake = Faker()
    fake.seed_instance(int(n*10))
    return fake.uuid4()

def random_account_number(n):
    global currentLocale
    print(currentLocale)
    fake = Faker(currentLocale)
    fake.seed_instance(int(n*10))
    return fake.iban()


def date_added(n):
    fake = Faker()
    fake.seed_instance(int(n*10))
    return fake.date_between(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

def critical_account(n):
    return  random.choice([True, False])


duckdb.create_function("uuid", random_uuid, [DOUBLE], VARCHAR)
duckdb.create_function("account_number", random_account_number, [DOUBLE], VARCHAR)
duckdb.create_function("date_added", date_added, [DOUBLE], DATE)
duckdb.create_function("critical_account", critical_account, [DOUBLE], VARCHAR)


numberOfRecords=50
numberOfDatasets=3
account_format="IBAN"

bank_id=["IZXVGB23BWP","QPSBDEB1","MRWNGBXX8ZX"]
for x in range(numberOfDatasets):
    currentLocale=locales[x]
    print(currentLocale)
    res = duckdb.sql("COPY (SELECT uuid(i) as account_uuid, account_number(i) as account_number, '"+account_format+"' as account_format, '"+bank_id[x]+"' as bank_id, date_added(i) as date_added, critical_account(i) as critical_account  FROM generate_series(1, "+str(numberOfRecords)+") s(i)) TO 'data/participant_"+str(x)+"/mule_accounts_clear.parquet'  (FORMAT 'parquet')")


#generate encrypted files
keys = ["GZs0DsMHdXr39mzkFwHwTHvCuUlID3HB","8SX9rT9VSHohHgEz2qRer5oCoid2RUAS","DrRLoOybRrUUANB9fkhHU9AZ7g4NKkMs"]
for x in range(numberOfDatasets):
    key = keys[x]
    keyName="dataset"+str(x)
    res=duckdb.sql("PRAGMA add_parquet_key('"+keyName+"','"+key+"')")
    res=duckdb.sql("COPY (SELECT * FROM './data/participant_"+str(x)+"/mule_accounts_clear.parquet') TO './data/participant_"+str(x)+"/mule_accounts.parquet' (ENCRYPTION_CONFIG {footer_key: '"+keyName+"'})")
    df = duckdb.sql("SELECT * FROM read_parquet('data/participant_"+str(x)+"/mule_accounts.parquet', encryption_config ={footer_key: '"+keyName+"'})").df()
    print (df)
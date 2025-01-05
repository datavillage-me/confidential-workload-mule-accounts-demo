import duckdb
import random
import os
import datetime
import numpy as np 

from duckdb.typing import *
from faker import Faker

# {
#     "suspicious-accounts": {
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
#             "reporter_bic": {
#                 "type": "varchar",
#                 "required": true,
#                 "pii": true,
#                 "classification": "sensitive"
#             },
#             "date_added": {
#                 "type": "date",
#                 "required": true,
#             }
#         }
#     }
# }




# Locales for Europe, the UK, and North America

locales = [
    # Europe
    'de_DE',  # German (Germany)
    'fr_BE',  # French (Belgium)

    # UK
    'en_GB',  # English (United Kingdom)
]

currentLocale=[]


def random_uuid(n):
    fake = Faker()
    fake.seed_instance(int(n*10))
    return fake.uuid4()

def random_account_number(n):
    i=random.randrange(0, 3)
    global currentLocale
    currentLocale=np.append(currentLocale,locales[i])
    fake = Faker(currentLocale[int(n)])
    fake.seed_instance(int(n*10))
    return fake.iban()

def random_bank_id(n,report_bic):
    global currentLocale
    fake = Faker(currentLocale[int(n)])
    fake.seed_instance(int(n*10))
    bank_id=fake.swift()
    return bank_id

def random_name(n):
    return  ""

def random_adress(n):
    return  ""

def random_social_security_number(n):
    return  ""

def date_added(n):
    fake = Faker()
    fake.seed_instance(int(n*10))
    return fake.date_between(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))


duckdb.create_function("uuid", random_uuid, [DOUBLE], VARCHAR)
duckdb.create_function("account_number", random_account_number, [DOUBLE], VARCHAR)
duckdb.create_function("bank_id", random_bank_id, [DOUBLE,VARCHAR], VARCHAR)
duckdb.create_function("name", random_name, [DOUBLE], VARCHAR)
duckdb.create_function("adress", random_adress, [DOUBLE], VARCHAR)
duckdb.create_function("social_security_number", random_social_security_number, [DOUBLE], VARCHAR)
duckdb.create_function("date_added", date_added, [DOUBLE], DATE)


numberOfRecords=200
numberOfDatasets=3
account_format="IBAN"

reporter_bic=["REPORTER_1","REPORTER_2","REPORTER_3"]
# for x in range(numberOfDatasets):
#     i=random.randrange(0, 3)
#     currentLocale=locales[i]
#     res = duckdb.sql("COPY (SELECT uuid(i) as account_uuid, account_number(i) as account_number, '"+account_format+"' as account_format, bank_id(i,'"+reporter_bic[x]+"') as bank_id, '"+reporter_bic[x]+"' as reporter_bic,name(i+{x*numberOfRecords}) as name, adress(i+{x*numberOfRecords}) as adress, social_security_number(i+{x*numberOfRecords}) as social_security_number, date_added(i) as date_added,  FROM generate_series(1, "+str(numberOfRecords)+") s(i)) TO 'data/participant_"+str(x)+"/suspicious_accounts_clear.parquet'  (FORMAT 'parquet')")

# #get bank_id with at least 3 reported account
# df = duckdb.sql("SELECT bank_id,count(*) as total FROM read_parquet(['data/participant_0/suspicious_accounts_clear.parquet','data/participant_1/suspicious_accounts_clear.parquet','data/participant_2/suspicious_accounts_clear.parquet']) GROUP BY bank_id HAVING total > 2").df()

# bank_bic=[3,2,3]
# for x in range (numberOfDatasets):
#     bank_id=df.iloc[x]["bank_id"]
#     bank_bic[x]=bank_id
#     print(bank_bic[x])
#     query="SELECT * FROM read_parquet(['data/participant_"+str(x)+"/suspicious_accounts_clear.parquet'])"
#     duckdb.sql("CREATE OR REPLACE TABLE suspicious_accounts AS "+query) 
#     duckdb.sql("UPDATE suspicious_accounts SET reporter_bic='"+bank_id+"' WHERE reporter_bic='"+reporter_bic[x]+"'") 
#     duckdb.sql("DELETE FROM suspicious_accounts WHERE bank_id='"+bank_id+"'")
#     duckdb.sql("COPY suspicious_accounts TO 'data/participant_"+str(x)+"/suspicious_accounts_clear.parquet'  (FORMAT 'parquet')") 

# for x in range (numberOfDatasets):
#     df = duckdb.sql("SELECT * FROM read_parquet(['data/participant_0/suspicious_accounts_clear.parquet','data/participant_1/suspicious_accounts_clear.parquet','data/participant_2/suspicious_accounts_clear.parquet'])  WHERE bank_id ='"+bank_bic[x]+"'").df()
#     print (df)

#create 1 doublon on purpose to break quality test on duplicate entries
# query="SELECT * FROM read_parquet(['data/participant_0/suspicious_accounts_clear.parquet'])"
# duckdb.sql("CREATE OR REPLACE TABLE suspicious_accounts AS "+query) 
# duckdb.sql("DELETE FROM suspicious_accounts WHERE account_number='DE19073380389042496385'")
# duckdb.sql("INSERT INTO suspicious_accounts VALUES ('c4bf11bf-f279-4235-8cb3-023072ce1456','DE60707429263289558603','IBAN','BYPYDEQLIZM','IZXVGB23BWP','2024-06-23')") 
# duckdb.sql("COPY suspicious_accounts TO 'data/participant_0/suspicious_accounts_clear.parquet'  (FORMAT 'parquet')") 

#generate encrypted files
keys = ["GZs0DsMHdXr39mzkFwHwTHvCuUlID3HB","8SX9rT9VSHohHgEz2qRer5oCoid2RUAS","DrRLoOybRrUUANB9fkhHU9AZ7g4NKkMs"]
for x in range(numberOfDatasets):
    key = keys[x]
    keyName="dataset"+str(x)
    res=duckdb.sql("PRAGMA add_parquet_key('"+keyName+"','"+key+"')")
    res=duckdb.sql("COPY (SELECT * FROM './data/participant_"+str(x)+"/suspicious_accounts_clear.parquet') TO './data/participant_"+str(x)+"/suspicious_accounts.parquet' (ENCRYPTION_CONFIG {footer_key: '"+keyName+"'})")
    df = duckdb.sql("SELECT * FROM read_parquet('data/participant_"+str(x)+"/suspicious_accounts.parquet', encryption_config ={footer_key: '"+keyName+"'})").df()
    print (df)


#export files to CSV
for x in range(numberOfDatasets):
    keyName="dataset"+str(x)
    res=duckdb.sql("COPY (SELECT * FROM './data/participant_"+str(x)+"/suspicious_accounts_clear.parquet') TO './data/participant_"+str(x)+"/suspicious_accounts.csv'")

# query="SELECT COUNT(*) as row_count FROM read_parquet(['data/participant_0/suspicious_accounts_clear.parquet']) where account_number='DE60707429263289558603' GROUP BY account_number"
# df=duckdb.sql(query).df()
# print(df)
#temporary - need to move into section above 
# bank_bic=["IZXVGB23BWP","QPSBDEB1","MRWNGBXX8ZX"]
# for x in range (numberOfDatasets):
#     query="SELECT * FROM read_parquet(['data/participant_"+str(x)+"/suspicious_accounts_clear.parquet'])"
#     duckdb.sql("CREATE OR REPLACE TABLE suspicious_accounts AS "+query) 
#     for bic in bank_bic:
#         if bic!= bank_bic[x]:
#             if "GB" in bic:
#                 locale="GB"
#             elif "DE" in bic:
#                 locale="DE"
#             query=f"UPDATE suspicious_accounts SET bank_id = '{bic}' WHERE bank_id NOT IN ('IZXVGB23BWP','QPSBDEB1','MRWNGBXX8ZX') AND account_number IN (SELECT account_number FROM suspicious_accounts WHERE SUBSTR(account_number, 1, 2) = '{locale}' LIMIT 10)";
#             duckdb.sql(query) 
#         duckdb.sql("COPY suspicious_accounts TO 'data/participant_"+str(x)+"/suspicious_accounts_clear.parquet'  (FORMAT 'parquet')") 
    # query="SELECT * FROM suspicious_accounts WHERE bank_id='IZXVGB23BWP'"
    # df=duckdb.sql(query).df()
    # print(df)



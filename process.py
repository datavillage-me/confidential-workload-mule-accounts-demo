"""
This project provides a demo of a confidential workload to share mule accounts.
The confidential workload handles 3 events: one to trigger the data holders' data quality checks, one to trigger selection of suspicious accounts and one to trigger check if an account is confirmed mule account
"""

import logging
import time
import yaml
import os
import json
import duckdb
from datetime import datetime
import base64
import shutil

from dv_utils import default_settings, Client, ContractManager,SecretManager,audit_log,LogLevel

logger = logging.getLogger(__name__)

# let the log go to stdout, as it will be captured by the cage operator
logging.basicConfig(
    level=default_settings.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

keys_input_dir = "/resources/data"
#keys_input_dir = "demo-keys"

# define an event processing function
def event_processor(evt: dict):
    """
    Process an incoming event
    Exception raised by this function are handled by the default event listener and reported in the logs.
    """
    
    logger.info(f"Processing event {evt}")

    # dispatch events according to their type
    evt_type =evt.get("type", "")

    if evt_type == "CHECK_DATA_QUALITY":
        # use the CHECK_DATA_QUALITY event processor dedicated function
        logger.info(f"Use the check data quality event processor")
        check_data_quality_contracts_event_processor(evt)
    elif evt_type == "GET_SUSPICIOUS_ACCOUNTS":
        # use the GET_SUSPICIOUS_ACCOUNTS event processor dedicated function
        logger.info(f"Use the get suspicious accounts event processor")
        get_suspicous_accounts_event_processor(evt)
    elif evt_type == "CHECK_MULE_ACCOUNT":
        # use the CHECK_MULE_ACCOUNT event processor dedicated function
        logger.info(f"Use the check mule account event processor")
        check_mule_account_event_processor(evt)
    else:
        generic_event_processor(evt)


def generic_event_processor(evt: dict):
    # push an audit log to reccord for an event that is not understood
    logger.info(f"Received an unhandled event {evt}")

def check_data_quality_contracts_event_processor(evt: dict):
    #audit logs are generated by the dv_utils sdk
    try:
        contractManager=ContractManager()
        test_results=contractManager.check_contracts_for_collaboration_space(default_settings.collaboration_space_id)

        #get data contracts from all data consumers
        data_contracts=contractManager.get_contracts_for_collaboration_space(default_settings.collaboration_space_id,Client.DATA_CONSUMER_COLLABORATOR_ROLE_VALUE)
        #Create in memory duckdb (encrypted memory on confidential computing)
        con = duckdb.connect(database=":memory:")
        
        #Add connector settings to duckdb con for all data contracts and export test results linked to the right client
        #TODO need to add the client_id in a contract within a collaboration space... to be discussed with the team
        client=Client()
        participants=client.get_list_of_participants(default_settings.collaboration_space_id,None)
        model_key="quality_checks"
        for data_contract in data_contracts:
            target_id=data_contract.data_descriptor_id
            target_client_id = next(
                (item["clientId"] for item in participants if "dataDescriptors" in item and any(dd["id"] == target_id for dd in item["dataDescriptors"])),
                None
            )
            for test_result_descriptor_id in test_results:
                source_client_id = next(
                    (item["clientId"] for item in participants if "dataDescriptors" in item and any(dd["id"] == test_result_descriptor_id for dd in item["dataDescriptors"])),
                    None
                )
                if target_client_id==source_client_id:
                    con = data_contract.connector.add_duck_db_connection(con)
                    con.sql(data_contract.export_contract_to_sql_create_table(model_key))
                    check_results=test_results[test_result_descriptor_id]
                    for check_result in check_results:
                        description=check_result
                        timestamp=now = datetime.now()
                        formated_now = now.strftime('%Y-%m-%dT%H:%M:%SZ')
                        check_result_json=test_results[test_result_descriptor_id][check_result]
                        hasErrors=check_result_json["hasErrors"]
                        hasWarnings=check_result_json["hasWarnings"]
                        hasFailures=check_result_json["hasFailures"]
                        query="INSERT INTO "+model_key+" VALUES ('"+description+"','"+formated_now+"',"+str(hasErrors)+","+str(hasWarnings)+","+str(hasFailures)+",'"+str(json.dumps(check_result_json).replace("'","''"))+"')"
                        con.sql(query)
                    data_contract.connector.export_signed_output_duckdb(model_key,default_settings.collaboration_space_id)
    except Exception as e:
        logger.error(e)

def get_suspicous_accounts_event_processor(evt: dict):
    try:
        model_key="suspicious_accounts"
        logger.info(f"---------------------------------------------------------")
        logger.info(f"|                    START PROCESSING                   |")
        logger.info(f"|                                                       |")
        start_time = time.time()
        logger.info(f"|    Start time:  {start_time} secs               |")
        logger.info(f"|                                                       |")
        audit_log(f"Start processing event: {evt.get('type', '')}.")

        #load parameters
        bank_id= evt.get("bank_id", "")
        allowed_bank_ids=default_settings.config("ALLOWED_BANK_IDS", default="", cast=str)
        allowed_bank_ids_list=None
        if allowed_bank_ids!=None:
            allowed_bank_ids_list=allowed_bank_ids.split(",")
        if bank_id==None or not(bank_id in allowed_bank_ids_list):
            audit_log(f"Bank identifier empty or not valid.")
            raise Exception("Bank identifier empty or not valid")

        logger.info(f"| 1. Get data contracts                                 |")
        logger.info(f"|                                                       |")

        #Connect in memory duckdb (encrypted memory on confidential computing)
        con = duckdb.connect(database=":memory:")

        collaboration_space_id=default_settings.collaboration_space_id
        contractManager=ContractManager()
        data_contracts=contractManager.get_contracts_for_collaboration_space(collaboration_space_id)

        logger.info(f"| 2. Load suspicious accounts from data sources         |")
        logger.info(f"|                                                       |")
        #DELETE TABLE aggregated_suspicious_accounts if exist
        con.execute("DROP TABLE IF EXISTS aggregated_suspicious_accounts")
        for data_contract in data_contracts:
            # Add DuckDB connection for the current data contract
            con = data_contract.connector.add_duck_db_connection(con)
            
            # Construct the query for the current data contract
            query = f"SELECT * FROM {data_contract.connector.get_duckdb_source(model_key)} WHERE bank_id='{bank_id}'"
            
            # Execute query and append or create table
            tables_list=[item[0] for item in con.execute("SHOW TABLES").fetchall()]
            if 'aggregated_suspicious_accounts' not in tables_list:
                con.execute(f"CREATE TABLE aggregated_suspicious_accounts AS {query}")
                audit_log(f"Read suspicious_accounts from: {data_contract.data_descriptor_id}.")
            else:
                con.execute(f"INSERT INTO aggregated_suspicious_accounts {query}")
                audit_log(f"Read suspicious_accounts from: {data_contract.data_descriptor_id}.")
        
        logger.info(f"| 3. Export suspicious accounts                         |")
        logger.info(f"|                                                       |")
        #get data contracts from all data consumers
        data_contracts=contractManager.get_contracts_for_collaboration_space(default_settings.collaboration_space_id,Client.DATA_CONSUMER_COLLABORATOR_ROLE_VALUE)
        #TODO need to add the client_id in a contract within a collaboration space... to be discussed with the team
        client=Client()
        participants=client.get_list_of_participants(default_settings.collaboration_space_id,None)
        export_model_key="suspicious_accounts"
        for data_contract in data_contracts:
            #Add connector settings to duckdb con for all data contracts and export test results linked to the right client
            con = data_contract.connector.add_duck_db_connection(con)
            target_id=data_contract.data_descriptor_id
            target_client_id = next(
                (item["clientId"] for item in participants if "dataDescriptors" in item and any(dd["id"] == target_id for dd in item["dataDescriptors"])),
                None
            )
            #export only if queried bank_id is the target_client_id
            if bank_id==default_settings.config("CLIENT_"+target_client_id+"_BANK_ID", default="", cast=str):
                con.sql(data_contract.export_contract_to_sql_create_table(export_model_key))
                result_query=f"SELECT account_uuid,account_number,account_format,bank_id,ARRAY_AGG(DISTINCT reporter_bic) AS reporter_bic,ARRAY_AGG(DISTINCT date_added) AS date_added,count(*) as report_count FROM aggregated_suspicious_accounts WHERE bank_id='{bank_id}' GROUP BY account_uuid, account_number, account_format, bank_id"
                query=f"INSERT INTO {export_model_key} ({result_query})"
                con.sql(query)
                con.sql(f"SELECT * FROM {export_model_key}")
                data_contract.connector.export_signed_output_duckdb(export_model_key,default_settings.collaboration_space_id)
                audit_log(f"Suspicious_accounts exported to: {data_contract.data_descriptor_id}.")
        logger.info(f"|                                                       |")
        execution_time=(time.time() - start_time)
        logger.info(f"|    Execution time:  {execution_time} secs           |")
        logger.info(f"|                                                       |")
        logger.info(f"--------------------------------------------------------")
    except Exception as e:
        logger.error(e)

def check_mule_account_event_processor(evt: dict):
    try:
        model_key="mule_accounts"
        logger.info(f"---------------------------------------------------------")
        logger.info(f"|                    START PROCESSING                   |")
        logger.info(f"|                                                       |")
        start_time = time.time()
        logger.info(f"|    Start time:  {start_time} secs               |")
        logger.info(f"|                                                       |")
        audit_log(f"Start processing event: {evt.get('type', '')}.")

        #load parameters
        encrypted_and_encoded_account_number= evt.get("account_number", "")
        secretManager=SecretManager()
        account_number=secretManager.decrypt_encoded_string(encrypted_and_encoded_account_number)
        caller_bank_id=evt.get("caller_bank_id", "")

        logger.info(f"| 1. Get data contracts                                 |")
        logger.info(f"|                                                       |")

        #Connect in memory duckdb (encrypted memory on confidential computing)
        con = duckdb.connect(database=":memory:")

        collaboration_space_id=default_settings.collaboration_space_id
        contractManager=ContractManager()
        data_contracts=contractManager.get_contracts_for_collaboration_space(collaboration_space_id)

        logger.info(f"| 2. Load mule accounts from data sources               |")
        logger.info(f"|                                                       |")
        #DELETE TABLE aggregated_mule_accounts if exist
        con.execute("DROP TABLE IF EXISTS aggregated_mule_accounts")
        for data_contract in data_contracts:
            # Add DuckDB connection for the current data contract
            con = data_contract.connector.add_duck_db_connection(con)
            
            # Construct the query for the current data contract
            query = f"SELECT * FROM {data_contract.connector.get_duckdb_source(model_key)} WHERE account_number='{account_number}'"
            
            # Execute query and append or create table
            tables_list=[item[0] for item in con.execute("SHOW TABLES").fetchall()]
            if 'aggregated_mule_accounts' not in tables_list:
                con.execute(f"CREATE TABLE aggregated_mule_accounts AS {query}")
                audit_log(f"Read mule_accounts from: {data_contract.data_descriptor_id}.")
            else:
                con.execute(f"INSERT INTO aggregated_mule_accounts {query}")
                audit_log(f"Read mule_accounts from: {data_contract.data_descriptor_id}.")
        
        logger.info(f"| 3. Export mules accounts report                       |")
        logger.info(f"|                                                       |")
        #get data contracts from all data consumers
        data_contracts=contractManager.get_contracts_for_collaboration_space(default_settings.collaboration_space_id,Client.DATA_CONSUMER_COLLABORATOR_ROLE_VALUE)
        client=Client()
        participants=client.get_list_of_participants(default_settings.collaboration_space_id,None)
        #TODO need to add the client_id in a contract within a collaboration space... to be discussed with the team
        export_model_key="mule_accounts"
        for data_contract in data_contracts:
            #Add connector settings to duckdb con for all data contracts and export test results linked to the right client
            con = data_contract.connector.add_duck_db_connection(con)
            target_id=data_contract.data_descriptor_id
            target_client_id = next(
                (item["clientId"] for item in participants if "dataDescriptors" in item and any(dd["id"] == target_id for dd in item["dataDescriptors"])),
                None
            )
            #export only if queried bank_id is the target_client_id
            if caller_bank_id==default_settings.config("CLIENT_"+target_client_id+"_BANK_ID", default="", cast=str):
                con.sql(data_contract.export_contract_to_sql_create_table(export_model_key))
                result_query=f"SELECT * FROM aggregated_mule_accounts"
                query=f"INSERT INTO {export_model_key} ({result_query})"
                con.sql(query)
                df=con.sql(f"SELECT * FROM {export_model_key}").df()
                print(df)
                data_contract.connector.export_signed_output_duckdb(export_model_key,default_settings.collaboration_space_id)
                audit_log(f"Mule_accounts exported to: {data_contract.data_descriptor_id}.")
        logger.info(f"|                                                       |")
        execution_time=(time.time() - start_time)
        logger.info(f"|    Execution time:  {execution_time} secs           |")
        logger.info(f"|                                                       |")
        logger.info(f"--------------------------------------------------------")
    except Exception as e:
        logger.error(e)

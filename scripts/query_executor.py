from config.s3_client_config import *
import logging
import time

logging.basicConfig(level=logging.INFO)

WORKGROUP = None
DATABASE = None 

# create database and table in redshift
def run_query(sql):
    client = get_redshift_client()
    response = client.execute_statement(
        Database=DATABASE,
        WorkgroupName=WORKGROUP,
        Sql=sql
    )

    query_id = response["Id"]

    status = client.describe_statement(Id=query_id)
        
    if status["Status"] != "FINISHED":
        raise Exception(status)
    print("Query executed successfully")

    result = client.get_statement_result(Id=query_id)
    return result

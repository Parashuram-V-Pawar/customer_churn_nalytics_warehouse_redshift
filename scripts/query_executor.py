from config.s3_client_config import *
import logging
import time

logging.basicConfig(level=logging.INFO)

WORKGROUP = 'customer-churn-workgroup'
DATABASE = 'customer_churn' 

def run_query(sql):
    client = get_redshift_client()
    response = client.execute_statement(
        Database=DATABASE,
        WorkgroupName=WORKGROUP,
        Sql=sql
    )
    query_id = response["Id"]

    while True:
        status = client.describe_statement(Id=query_id)
        state = status["Status"]

        logging.info(f"Query Status: {state}")

        if state == "FINISHED":
            break
        elif state in ["FAILED", "ABORTED"]:
            raise Exception(status)

        time.sleep(2)

    print("Query executed successfully")

    if status.get("HasResultSet"):
        result = client.get_statement_result(Id=query_id)
        return result
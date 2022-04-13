import snowflake.connector
import logging
import pandas as pd
import os
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

load_dotenv()
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

#query_table("IS3107_NEWS_DATA", "NEWS_DATA", "NEWS_TABLE", "2022-01-01", "2022-03-31")
def query_table(db, schema, table, start_date, stop_date):
    snowflake.connector.paramstyle= 'qmark'

    conn = snowflake.connector.connect(
                    user=username,
                    password=password,
                    account="ts39829.ap-southeast-1",
                    warehouse="COMPUTE_WH",
                    database=db,
                    schema=schema
                    )

    curr = conn.cursor()

    sql_query = """SELECT * FROM {}.{} WHERE 
    DATE >= '{}' and DATE <= '{}'
    """.format(schema, table, start_date, stop_date)
    print(sql_query)

    result = curr.execute(sql_query)
    print("it ran!")
    df = pd.DataFrame.from_records(iter(result), columns=[x[0] for x in result.description])

    return df
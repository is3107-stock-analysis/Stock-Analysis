import snowflake.connector
import logging
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator


load_dotenv()
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

def insert_data(dataframe, db, schema, table):
    '''
    Function used by other methods for insertion of the 
    resultant dataframes into Snowflake.

    ----------------
    Parameters
    dataframe: Dataframe to be loaded into Snowflake.
    db: Database in Snowflake which houses the required schema
    schema: Schema in Snowflake which houses the required table
    table: Table in Snowflake which houses the required data
    '''

    ## insert into holdings table
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
    columns = dataframe.columns
    col_len = len(columns)

    for index,row in dataframe.iterrows():
        sql_query = "INSERT INTO {}.{} VALUES (".format(schema, table)

        for i in range(col_len):
            if isinstance(row[columns[i]], float):
                sql_query +=  str(row[columns[i]]) + ", "
            
            else:
                sql_query += "'"+ str(row[columns[i]]) + "', "
                

        sql_query = sql_query[:-2] + ')'
        print(sql_query)

        curr.execute(sql_query)

    conn.close()
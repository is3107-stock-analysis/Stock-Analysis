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
from news_webscraper.NewsScraper import NewsScraper
from sti_data_scraper.get_stock_data import get_data_for_multiple_stocks
from portfolio_decision_making.portfolio_optimization.optimization import get_optimized_portfolio
from portfolio_decision_making.portfolio_optimization.comparison_statistics import get_comparison_statistics
from sti_data_scraper.holdings_scraper import HoldingsScraper
from etl.data_cleaning import DataCleaning

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
    DATETIME >= {} and DATETIME <={}""".format(schema, table, start_date, stop_date)

    result = curr.execute(sql_query)
    df = pd.DataFrame.from_records(iter(result), columns=[x[0] for x in result.description])

    return df
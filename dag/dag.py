import snowflake.connector
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from news_webscraper.NewsScraper import NewsScraper
from sti_data_scraper.get_stock_data import get_data_for_multiple_stocks
from portfolio_decision_making.portfolio_optimization.optimization import get_optimized_portfolio
from portfolio_decision_making.portfolio_optimization.comparison_statistics import get_comparison_statistics

def helloWorld():
    test = NewsScraper()
    print("newsscraper success")
    print("Hello World")

# def query():
#     conn = snowflake.connector.connect(
#                     user="*******",
#                     password="*******",
#                     account="ts39829.ap-southeast-1",
#                     warehouse="COMPUTE_WH",
#                     database="IS3107_NEWS_DATA",
#                     schema="TEST_SCHEMA"
#                     )
#     cur = conn.cursor()
#     cur.execute("select * from test_article")
#     query_id = cur.sfqid
#     cur.get_results_from_sfqid(query_id)
#     results = cur.fetchall()
#     print(f'{results[0]}')

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
# create_insert_query = [
#     """create table public.test_table (amount number);""",
#     """insert into public.test_table values(1),(2),(3);""",
# ]

# def row_count(**context):
#     dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
#     result = dwh_hook.get_first("select count(*) from public.test_table")
#     logging.info("Number of rows in `public.test_table`  - %s", result[0])


with DAG(dag_id="hello_world_dag",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:

        """
        Scrape all required data
        """

        task1 = PythonOperator(
        task_id="hello_world",
        python_callable=helloWorld)


        # ## cursor method for querying data from snowflake
        # task2 = PythonOperator(
        #     task_id="snowflake_query",
        #     python_callable=query
        # )

        # ## conn method in airflow
        # task2 = SnowflakeOperator(
        # task_id="snowflake_create_query",
        # sql=create_insert_query,
        # snowflake_conn_id="snowflake_conn")

        get_stocks = PythonOperator(
        task_id="scrape_stocks_data",
        python_callable=get_data_for_multiple_stocks, 
        op_kwargs={"start_date":"2022-01-01", "end_date":"2022-04-01"}
        )

        """
        Perform Transformation Tasks
        """
        ## NEED to do cleaning of stock/news data first, then push into DB


        """
        Load into dataa warehouse
        """


        """
        Portfolio Analysis Section
        """

        #NEED to load stock data here

        #we minimize risk with while placing more emphasis on returns
        get_optimized_portfolio= PythonOperator(
        task_id="optimize_portfolio",
        python_callable=get_optimized_portfolio, 
        op_kwargs={"returns_scale":0.0001}
        )

        #Get the optimized portfolio statistics
        get_comparison_statistics = PythonOperator(
            task_id="get_statistics", 
            python_callable=get_comparison_statistics
        )

    
task1>>get_stocks>>get_optimized_portfolio>>get_comparison_statistics
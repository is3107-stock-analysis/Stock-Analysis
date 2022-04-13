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
from sql_helpers.sql_upload import insert_news, insert_holdings
from sql_helpers.sql_query import query_table

load_dotenv()
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

def helloWorld():
    test = NewsScraper()
    print("newsscraper success")
    print("Hello World")


# def load_data(to_db, to_table = ""):
#     if to_db == 'news_data':
#         insert_news()
    
#     elif to_db == 'results':
#         insert_reweighting()

#     elif to_db == 'stocks':
#         if to_table == 'holdings':
#             insert_holdings()
        
#         elif to_table == 'stocks':
#             insert_stocks()

#         elif to_table == 'portfolio_statistics':
#             insert_portfolio_statistics()

def query_test():
    df = query_table("IS3107_STOCKS_DATA", "STOCKS_DATA", "PORTFOLIO_STATISTICS", '2022-01-01', '2022-03-31')
    print(df.head())




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


        get_stocks = PythonOperator(
        task_id="scrape_stocks_data",
        python_callable=get_data_for_multiple_stocks, 
        op_kwargs={"start_date":"2022-01-01", "end_date":"2022-04-01"}
        )

        """
        Perform Transformation Tasks
        """
        ## NEED to do cleaning of stock/news data first, then push into DB

        test_query = PythonOperator(
            task_id = "test_query",
            python_callable = query_test
        )

        """
        Load into data warehouse
        """
        # insert_holdings = PythonOperator(
        #     task_id = "insert_holdings",
        #     python_callable = load_data,
        #     op_kwargs={"to_db":'stocks' , "to_table":'holdings'}
        # )


        # insert_news_data = PythonOperator(
        #     task_id="insert_news", 
        #     python_callable= load_data,
        #     op_kwargs={"to_db":'news_data'}
        # )

        # insert_results_data = PythonOperator(
        #     task_id="insert_results", 
        #     python_callable= load_data,
        #     op_kwargs={"to_db":'results', "to_table":'REWEIGHTING'}
        # )


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

        ##load it into snowflake
        #fucniton call

        #Get the optimized portfolio statistics
        get_comparison_statistics = PythonOperator(
            task_id="get_statistics", 
            python_callable=get_comparison_statistics
        )

    
task1>>get_stocks>>test_query>>get_optimized_portfolio>>get_comparison_statistics
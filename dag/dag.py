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
from portfolio_decision_making.portfolio_optimization.suggested_reweightings import suggested_reweightings
# from portfolio_decision_making.sentiment_analysis import SentimentAnalysis
from sti_data_scraper.holdings_scraper import HoldingsScraper
from sql_helpers.sql_upload import insert_data
from sql_helpers.sql_query import query_table


load_dotenv()
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:

        """
        Scrape & Load all required data
        """
        insert_holdings = PythonOperator(
            task_id = "insert_holdings",
            python_callable = HoldingsScraper.scrape_holdings
        )


        get_stocks = PythonOperator(
        task_id="scrape_stocks_data",
        python_callable=get_data_for_multiple_stocks
        )

        # insert_news = PythonOperator(
        #     task_id = 'insert_news',
        #     python_callable = NewsScraper.scrape_news
        # )

        """
        Portfolio Analysis Section
        """

        #we minimize risk with while placing more emphasis on returns
        get_optimized_portfolio= PythonOperator(
        task_id="optimize_portfolio",
        python_callable=get_optimized_portfolio, 
        op_kwargs={"returns_scale":0.0001}
        )

        get_adjustment = PythonOperator(
            task_id="suggest_reweight",
            python_callable=suggested_reweightings
        )

        ##SA
        # sentiment_analysis = PythonOperator(
        #     task_id = "sentiment_analysis",
        #     python_callable=SentimentAnalysis.get_sentiments
        # )

        #Get the optimized portfolio statistics
        get_comparison_statistics = PythonOperator(
            task_id="get_statistics", 
            python_callable=get_comparison_statistics
        )



insert_holdings>>get_stocks>>get_optimized_portfolio>>get_adjustment>>get_comparison_statistics
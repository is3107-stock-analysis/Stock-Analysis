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


load_dotenv()
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

def helloWorld():
    test = NewsScraper()
    print("newsscraper success")
    print("Hello World")


def load_data(to_db, to_table):
    if to_db == 'news_data':

        #probably should be a function tt scrapes the thing or smth...
        holdings = HoldingsScraper.scrape_holdings()[1]
        print('holdings scraped')
        companies = []
        tickers = []
        for idx, rows in holdings.iterrows():
            companies.append(rows['company'])
            tickers.append(rows['ticker'])

        dataframe = NewsScraper.scrape_news(tickers, companies)
        print('news df created')
        print(dataframe.head())

        dataframe = DataCleaning.start_clean(dataframe)
        print('cleaned')

        insert_news(dataframe, to_table)

    elif to_db == 'results':

        #probably should be a function tt scrapes the thing or smth...
        dataframe = pd.DataFrame({
        'ticker':['apl', 'pe'],
        'optimal_weight': [0.6, 0.4],
        'adjustment': [0.1, -0.1],
        'sentiment': ['good', 'bad'],
        })

        insert_results(dataframe, to_table)

    elif to_db == 'stocks_data':

        insert_stocks(df, to_table)

def insert_stocks(dataframe, to_table):
    snowflake.connector.paramstyle= 'qmark'

    conn = snowflake.connector.connect(
            user=username,
            password=password,
            account="ts39829.ap-southeast-1",
            warehouse="COMPUTE_WH",
            database="IS3107_STOCKS_DATA",
            schema="STOCKS_DATA"
            )
    
    curr = conn.cursor()
    
    if to_table == 'STOCKS_RETURN':
        #drop table first.
        curr.execute(
            "DROP TABLE IF EXISTS STOCKS_DATA.STOCKS_RETURN"
        )

        #extract column names
        column_names = list(dataframe.columns)

        #create table with column names
        create_query = "CREATE OR REPLACE TABLE STOCKS_DATA.STOCKS (DATE DATE, "

        for i in range(len(column_names)):
            create_query += str(column_names)[i] 
            create_query += " DOUBLE, "
        create_query += ')'

        curr.execute(create_query)

        #insert data

    elif to_table == 'PORTFOLIO_HOLDINGS':
        #confusion
        print('hello')

    elif to_table == 'PORTFOLIO_STATISTICS':

        for index,row in dataframe.iterrows():
            PORTFOLIO = row['ticker']
            SHARPE = row['optimal_weight']
            VOLATILITY = row['adjustment']

            curr.execute(
            "INSERT INTO STOCKS_DATA." + to_table + " VALUES (?, ?, ?)",
            (PORTFOLIO, SHARPE, VOLATILITY)
            )

    conn.close()



def insert_results(dataframe, to_table):
    snowflake.connector.paramstyle= 'qmark'

    conn = snowflake.connector.connect(
                    user=username,
                    password=password,
                    account="ts39829.ap-southeast-1",
                    warehouse="COMPUTE_WH",
                    database="IS3107_RESULTS",
                    schema="FINAL_OUTPUT"
                    )

    curr = conn.cursor()

    for index,row in dataframe.iterrows():
        TICKER = row['ticker']
        OP_WEIGHT = row['optimal_weight']
        ADJ = row['adjustment']
        SENTIMENT = row['sentiment']

        curr.execute(
        "INSERT INTO FINAL_OUTPUT." + to_table + " VALUES (?, ?, ?, ?)",
        (TICKER, OP_WEIGHT, ADJ, SENTIMENT)
        )
    
    conn.close()

def insert_news(dataframe, to_table):
    snowflake.connector.paramstyle= 'qmark'

    conn = snowflake.connector.connect(
                    user=username,
                    password=password,
                    account="ts39829.ap-southeast-1",
                    warehouse="COMPUTE_WH",
                    database="IS3107_NEWS_DATA",
                    schema="NEWS_DATA"
                    )

    curr = conn.cursor()

    for index,row in dataframe.iterrows():
        TITLE = row['title_no_newline_ellipse']
        ARTICLE_DATE = row['date']
        LINK = row['link']
        COMPANY = row['company']
        TICKER = row['ticker']

        curr.execute(
        "INSERT INTO NEWS_DATA." + to_table + " VALUES (?, ?, ?, ?, ?)",
        (TITLE, ARTICLE_DATE, LINK, COMPANY, TICKER)
        )

    conn.close()


# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
# create_insert_query = [
#     """create table public.test_table (amount number);""",
#     """insert into public.test_table values(1),(2),(3);""",
# ]


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


        """
        Load into data warehouse
        """
        insert_news_data = PythonOperator(
            task_id="insert_news", 
            python_callable= load_data,
            op_kwargs={"to_db":'news_data', "to_table":'NEWS_TABLE'}
        )

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

    
task1>>get_stocks>>insert_news_data>>get_optimized_portfolio>>get_comparison_statistics
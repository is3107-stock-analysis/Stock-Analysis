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
from news_webscraper.NewsScraper import NewsScraper
from sti_data_scraper.get_stock_data import get_data_for_multiple_stocks
from portfolio_decision_making.portfolio_optimization.optimization import get_optimized_portfolio
from portfolio_decision_making.portfolio_optimization.comparison_statistics import get_comparison_statistics
from sti_data_scraper.holdings_scraper import HoldingsScraper
from etl.data_cleaning import DataCleaning
from sql_query import query_table

load_dotenv()
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

def insert_holdings():
    dataframe = HoldingsScraper.scrape_holdings()
    print('holdings scraped')

    ## insert into holdings table
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

    for index,row in dataframe.iterrows():
        COMPANY = row['company']
        TICKER = row['ticker']
        TRUE_WEIGHT = row['true_weights']
        TOP10_WEIGHT = row['top_10_weights']
        DATE_QUARTER = row['quarter']

        curr.execute(
        "INSERT INTO STOCKS_DATA.STOCKS_HOLDINGS VALUES (?, ?, ?, ?, ?)",
        (COMPANY, TICKER, TRUE_WEIGHT, TOP10_WEIGHT, DATE_QUARTER)
        )

    conn.close()

def insert_news():


    # Get current date-time.
    now = datetime.now()
    start_date, end_date = get_quarter_start_end(now)
    holdings = query_table('IS3107_STOCKS_DATA', 'STOCKS_DATA', 'STOCKS_HOLDINGS', start_date, end_date) #query from the holdings table

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
        "INSERT INTO NEWS_DATA.NEWS_TABLE VALUES (?, ?, ?, ?, ?)",
        (TITLE, ARTICLE_DATE, LINK, COMPANY, TICKER)
        )

    conn.close()


def get_quarter_start_end(dt):
    year = str(dt.year)
    quarter = ((now.month-1)//3+1) - 1

    if quarter == 1:
        start_date = year+ '-01-01'
        end_date = year+ '-03-31'
        return start_date, end_date
    
    elif quarter == 2:
        start_date = year+ '-04-01'
        end_date = year+ '-06-30'
        return start_date, end_date
    
    elif quarter == 3:
        start_date = year+ '-07-01'
        end_date = year+ '-09-30'
        return start_date, end_date
    
    elif quarter == 4:
        start_date = year+ '-10-01'
        end_date = year+ '-12-31'
        return start_date, end_date
    

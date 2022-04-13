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

def insert_news():
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


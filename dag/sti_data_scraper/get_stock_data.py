import sys, os
sys.path.append(os.path.abspath(os.path.join('..', 'sql_helpers')))

import pandas as pd
from pandas_datareader.data import DataReader
from copy import deepcopy
import math
import numpy as np
import pandas as pd
from pandas_datareader.data import DataReader
from scipy.stats import norm
from math import sqrt
from sql_helpers.sql_query import query_table

"""
Get stocks data
"""

def get_data_for_multiple_stocks(ti, start_date, end_date):
    '''
    Obtain stocks information (Date, OHLC, Volume and Adjusted Close). 
    Uses Pandas DataReader to make an API Call to Yahoo Finance and download the data directly.
    Computes other values - Log Return and Arithmetic Return.
    
    Input: List of Stock Tickers
    Output: A dictionary of dataframes for each stock
    '''

    stock_holdings = query_table("IS3107_STOCKS_DATA", "STOCKS_DATA", "STOCK_HOLDINGS", "2022-01-01", "2022-03-31")
    print("query ran!")
    print(stock_holdings.head())
    tickers = list(stock_holdings.TICKER)
    # read in stock data
    s = DataReader(tickers[0], 'yahoo', start_date, end_date)[["Adj Close"]]
    # get log returns
    s[tickers[0]] = np.log(s['Adj Close']/s['Adj Close'].shift(1))
    
    stocks = s[[tickers[0]]]
    
    for ticker in tickers[1:]:
        s = DataReader(ticker, 'yahoo', start_date, end_date)
        s[ticker] = np.log(s['Adj Close']/s['Adj Close'].shift(1))
        stocks[ticker] = s[ticker]
        
    # skip first row that will be na, and fillna by 0 incase there are trading halts on specific days
    stocks = stocks.iloc[1:].fillna(0)
    # 1. Keep date
    # 2. Get all of the columns (as a list) and then we try and do the company col 
    # 3. index the df to get the stock_returns 
    stocks_pivoted = df_table_converter(stocks)
    # TODO insert to db


    ### Push into XCOM 
    ti.xcom_push(key="stocks_returns_df", value=stocks_pivoted.to_json())

    def df_table_converter(df_stocks):
        all_cols = df_stocks.columns
        date_cols = all_cols[0]
        ticker_columns = all_cols[1:]

        #[date, ticker, return]
        ticker_row_info =[]


        for i in range(len(df_stocks.index)):
            date = df_stocks.loc[i,date_cols]
            for ticker in ticker_columns:
                ticker_returns = df_stocks.loc[i,ticker]
                ticker_row_info.append([date, ticker, ticker_returns])

        df_stocks_pivoted = pd.DataFrame(ticker_row_info, columns =['date', 'ticker', 'stock_returns'])
        return df_stocks_pivoted

    return stocks.to_json() 
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

    # TODO Add in date to this data

    ### Push into XCOM 
    ti.xcom_push(key="stocks_returns_df", value=stocks.to_json())
    
    return stocks.to_json() 


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
from datetime import date
from dateutil.relativedelta import relativedelta

"""
Get stocks data
"""

def get_data_for_multiple_stocks(ti):
    '''
    Obtain stocks information (Date, OHLC, Volume and Adjusted Close). 
    Uses Pandas DataReader to make an API Call to Yahoo Finance and download the data directly.
    Computes other values - Log Return and Arithmetic Return.
    
    Input: List of Stock Tickers
    Output: A dictionary of dataframes for each stock
    '''

    today = date.today().strftime("%m/%d/%Y")

    three_months_ago_d = date.today() + relativedelta(months=-3)
    three_months_ago = three_months_ago_d.strftime("%m/%d/%Y")
    #three_months_ago_minus_one_day_d = three_months_ago_d + relativedelta(days=-1)
    #three_months_ago_minus_one_day = three_months_ago_minus_one_day_d.strftime("%m/%d/%Y")

    two_months_ago_d = date.today() + relativedelta(months=-2)
    two_months_ago = two_months_ago_d.strftime("%m/%d/%Y")
    two_months_ago_minus_one_day_d = two_months_ago_d + relativedelta(days=-1)
    two_months_ago_minus_one_day = two_months_ago_minus_one_day_d.strftime("%m/%d/%Y")

    one_months_ago_d = date.today() + relativedelta(months=-1)
    one_months_ago = one_months_ago_d.strftime("%m/%d/%Y")
    one_months_ago_minus_one_day_d = one_months_ago_d + relativedelta(days=-1)
    one_months_ago_minus_one_day = one_months_ago_minus_one_day_d.strftime("%m/%d/%Y")

    start_date = three_months_ago_d
    end_date = date.today()


    stock_holdings = query_table("IS3107_STOCKS_DATA", "STOCKS_DATA", "STOCK_HOLDINGS", start_date, end_date)
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

    ##huimin transform
    ##upload function to table
    
    return stocks.to_json() 


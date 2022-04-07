import cvxpy as cvx
import numpy as np
import pandas as pd
from pandas_datareader.data import DataReader
from copy import deepcopy
import math
import numpy as np
import pandas as pd
from pandas_datareader.data import DataReader
from scipy.stats import norm
from math import sqrt

"""
Get stocks data
"""

def get_data_for_multiple_stocks(start_date, end_date):
    '''
    Obtain stocks information (Date, OHLC, Volume and Adjusted Close). 
    Uses Pandas DataReader to make an API Call to Yahoo Finance and download the data directly.
    Computes other values - Log Return and Arithmetic Return.
    
    Input: List of Stock Tickers
    Output: A dictionary of dataframes for each stock
    '''

    # microsoft, goldman sachs, disney, macdonalds, johnsen and johnsen, gold
    tickers = ["D05.SI", "U11.SI", "V03.SI", "A17U.SI", "9CI.SI"]
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
    print(stocks.iloc[1:].fillna(0))

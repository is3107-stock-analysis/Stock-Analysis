import numpy as np
import pandas as pd

import sys, os
sys.path.append(os.path.abspath(os.path.join('..', 'sql_helpers')))

from sql_helpers.sql_query import query_table
from sql_helpers.sql_upload import insert_data

def suggested_reweightings(ti):
    """
    Compares and outputs the recommended adjusted weights
    """
    optimized_df = pd.read_json(ti.xcom_pull(key="optimized_weights", task_ids=["optimize_portfolio"])[0])

    # load weights from stock holdings
    stock_holdings = query_table("IS3107_STOCKS_DATA", "STOCKS_DATA", "STOCK_HOLDINGS", "2022-01-01", "2022-03-31")
    original_weights = list(stock_holdings.TOP10_WEIGHT)
    tickers = list(stock_holdings.TICKER)
    reweighting = pd.DataFrame(columns = ["Ticker", "Adjustment"])
    reweighting["Ticker"]= tickers
    
    for ticker in tickers:
        reweighting.loc[reweighting["Ticker"]==ticker,"Adjustment"] = (original_weights[original_weights["ticker"]==ticker].weight) -optimized_df[ticker] 

    ### Push into XCOM 
    ti.xcom_push(key="reweighting", value=reweighting.to_json())

    



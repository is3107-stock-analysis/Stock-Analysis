import numpy as np
import pandas as pd

def suggested_reweightings(ti):
    """
    Compares and outputs the recommended adjusted weights
    """
    optimized_df = pd.read_json(ti.xcom_pull(key="optimized_weights", task_ids=["optimize_portfolio"])[0])

    # load weights from stock holdings
    stock_holdings = query_table("IS3107_STOCKS_DATA", "STOCKS_DATA", "STOCK_HOLDINGS", "2022-01-01", "2022-03-31")
    original_weights = list(stock_holdings.TOP10_WEIGHT)
    reweighting = pd.DataFrame(columns = ["Ticker", "Adjustment"])
    reweighting["Ticker"]=tickers
    
    for ticker in tickers:
        reweighting.loc[reweighting["Ticker"]==ticker,"Adjustment"] = (original_weights[original_weights["ticker"]==ticker].weight) -optimized_df[ticker] 

    ### Push into XCOM 
    ti.xcom_push(key="reweighting", value=reweighting.to_json())



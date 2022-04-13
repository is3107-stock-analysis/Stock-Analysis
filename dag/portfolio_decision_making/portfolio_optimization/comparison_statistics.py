import numpy as np
import pandas as pd
import json

from datetime import date

def get_comparison_statistics(ti):
    """
    Gets the returns time series of the two portfolios & calculates volatility and Sharpe ratio for comparison
    """
    returns_df = pd.read_json(ti.xcom_pull(key="stocks_returns_df", task_ids=["scrape_stocks_data"])[0])

    # load weights from stock holdings
    stock_holdings = query_table("IS3107_STOCKS_DATA", "STOCKS_DATA", "STOCK_HOLDINGS", "2022-01-01", "2022-03-31")
    original_weights = list(stock_holdings.top10_weight)
    optimized_weights = json.loads(ti.xcom_pull(key="optimized_weights", task_ids=["optimize_portfolio"])[0])


    #original portfolio performance
    sti_benchmark_ret = (returns_df * original_weights).sum(axis = 1)
    sti_vol = sti_benchmark_ret.std(axis=0)* 260 ** 0.5
    sti_sharpe = sti_benchmark_ret.mean(axis = 0) / sti_benchmark_ret.std(axis = 0) * 260 ** 0.5

    #Returns of portfolio with new weights
    opt_portfolio_ret = (returns_df * optimized_weights).sum(axis = 1)
    opt_vol = opt_portfolio_ret.std(axis=0)* 260 ** 0.5
    opt_sharpe = opt_portfolio_ret.mean(axis = 0) / opt_portfolio_ret.std(axis = 0) * 260 ** 0.5

    
    stats = pd.DataFrame(columns=["Date", "Portfolio", "Sharpe", "Volatility"])
    date = str(date.today())
    stats.loc[len(stats)] = [date, "STI", sti_sharpe, sti_vol]
    stats.loc[len(stats)] = [date, "Optimized", opt_sharpe, opt_vol]

    # Save to xcoms to be combined with sentiment analysis output
    ti.xcom_push(key="comparison_statistics", value=stats)

    



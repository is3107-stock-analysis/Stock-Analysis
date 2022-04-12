import numpy as np
import pandas as pd
import json

def get_comparison_statistics(ti):
    """
    Gets the returns time series of the two portfolios & calculates volatility and Sharpe ratio for comparison
    """
    returns_df = pd.read_json(ti.xcom_pull(key="stocks_returns_df", task_ids=["scrape_stocks_data"])[0])
    # TODO to be replaced
    original_weights = [0.1954, 0.1318, 0.1259, 0.0616, 0.0491, 0.0338, 0.0315, 0.0305, 0.0302, 0.0287]
    optimized_weights = json.loads(ti.xcom_pull(key="optimized_weights", task_ids=["optimize_portfolio"])[0])


    #original portfolio performance
    sti_benchmark_ret = (returns_df * original_weights).sum(axis = 1)
    sti_vol = sti_benchmark_ret.std(axis=0)* 260 ** 0.5
    sti_sharpe = sti_benchmark_ret.mean(axis = 0) / sti_benchmark_ret.std(axis = 0) * 260 ** 0.5

    #Returns of portfolio with new weights
    opt_portfolio_ret = (returns_df * optimized_weights).sum(axis = 1)
    opt_vol = opt_portfolio_ret.std(axis=0)* 260 ** 0.5
    opt_sharpe = opt_portfolio_ret.mean(axis = 0) / opt_portfolio_ret.std(axis = 0) * 260 ** 0.5

    # TODO save returns df, vol, sharpe tables
    stats = pd.DataFrame(columns=["Portfolio", "Sharpe", "Volatility"])
    stats.loc[len(stats)] = ["STI", sti_sharpe, sti_vol]
    stats.loc[len(stats)] = ["Optimized", opt_sharpe, opt_vol]
    # Save to database
    stats.to_csv("statistics.csv")
    



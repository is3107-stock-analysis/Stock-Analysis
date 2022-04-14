import sys, os
sys.path.append(os.path.abspath(os.path.join('..', 'sql_helpers')))

import numpy as np
import pandas as pd
import json

from datetime import date
from dateutil.relativedelta import relativedelta
from sql_helpers.sql_query import query_table
from sql_helpers.sql_upload import insert_data


def get_comparison_statistics(ti):
    """
    Gets the returns time series of the two portfolios & calculates volatility and Sharpe ratio for comparison
    """

    today = date.today().strftime("%m/%d/%Y")

    one_months_ago_d = date.today() + relativedelta(months=-1)
    one_months_ago = one_months_ago_d.strftime("%m/%d/%Y")
    one_months_ago_minus_one_day_d = one_months_ago_d + relativedelta(days=-1)
    one_months_ago_minus_one_day = one_months_ago_minus_one_day_d.strftime("%m/%d/%Y")

    returns_df = pd.read_json(ti.xcom_pull(key="stocks_returns_df", task_ids=["scrape_stocks_data"])[0])

    # load weights from stock holdings
    stock_holdings = query_table("IS3107_STOCKS_DATA", "STOCKS_DATA", "STOCK_HOLDINGS", one_months_ago_d, date.today())
    original_weights = list(stock_holdings.TOP10_WEIGHT)
    optimized_weights = pd.read_json(ti.xcom_pull(key="optimized_weights", task_ids=["optimize_portfolio"])[0])

    #original portfolio performance
    sti_benchmark_ret = (returns_df * original_weights).sum(axis = 1)
    sti_vol = sti_benchmark_ret.std(axis=0)* 260 ** 0.5
    sti_sharpe = sti_benchmark_ret.mean(axis = 0) / sti_benchmark_ret.std(axis = 0) * 260 ** 0.5

    #Returns of portfolio with new weights
    opt_portfolio_ret = (returns_df * list(optimized_weights.Weight)).sum(axis = 1)
    opt_vol = opt_portfolio_ret.std(axis=0)* 260 ** 0.5
    opt_sharpe = opt_portfolio_ret.mean(axis = 0) / opt_portfolio_ret.std(axis = 0) * 260 ** 0.5

    
    stats = pd.DataFrame(columns=["Date", "Portfolio", "Sharpe", "Volatility"])
    today = str(date.today())
    stats.loc[len(stats)] = [today, "STI", sti_sharpe, sti_vol]
    stats.loc[len(stats)] = [today, "Optimized", opt_sharpe, opt_vol]

    insert_data(stats, "IS3107_STOCKS_DATA", "STOCKS_DATA", "PORTFOLIO_STATISTICS")

    



import cvxpy as cvx
import numpy as np
import pandas as pd
import json


def get_optimized_portfolio(ti, returns_scale = .0001, max_holding=0.5):
    """
    Function that takes in the returns series of assets, minimizes the utility function, and returns
    the portfolio weights
    
    Parameters
    ----------
    returns_df : pd.dataframe
        Dataframe containing log asset return series in each column
    
    returns_scale : float
        The scaling factor applied to the returns
        
    Returns
    -------
    x : np.ndarray
        A numpy ndarray containing the weights of the assets in the optimized portfolio
    """

    returns_df = pd.read_json(ti.xcom_pull(key="stocks_returns_df", task_ids=["scrape_stocks_data"])[0])

    # number of assets m
    returns = returns_df.T.to_numpy()
    
    # weights to optimize
    m = returns.shape[0]
    
    #covariance matrix of returns
    cov = np.cov(returns)
    
    # creating variable of weights to optimize
    x = cvx.Variable(m)
    
    # portfolio variance, in quadratic form
    portfolio_variance = cvx.quad_form(x, cov)
    total_return = returns_df.sum().to_numpy()
    
    # Element wise multiplication, followed up by sum of weights
    portfolio_return = sum(cvx.multiply(total_return, x))
    
    # objective function
    # We want to minimize variance and maximize returns. We can also minimize the negative of returns
    # Therefore, variance has to be a positive and returns have to be a negative.
    objective = cvx.Minimize(portfolio_variance - returns_scale * portfolio_return)
    
    # constraints
    # Long only, sum of weights equal to 1, no allocation to a single stock great than 50% of portfolio
    constraints = [x >= 0, sum(x) == 1, x <= max_holding]

    #use cvxpy to solve the objective
    problem = cvx.Problem(objective, constraints)
    #retrieve the weights of the optimized portfolio
    result = problem.solve()
    opt_weights = round(pd.Series(x.value, index = returns_df.columns), 2)
    optimized_weights=pd.DataFrame(opt_weights).reset_index()
    optimized_weights.columns=["Ticker", 'Weight']

    # Push into XCOM
    ti.xcom_push(key="optimized_weights", value=optimized_weights.to_json())
    return optimized_weights.to_json()


import sys, os
sys.path.append(os.path.abspath(os.path.join('..', 'sql_helpers')))

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date

from sql_helpers.sql_upload import insert_data


class HoldingsScraper:

    @staticmethod
    def scrape_holdings():
        """
        Scrapes STI's top 10 holdings from SSGA website. 
        Calculates the weight of each stock in the top 10 holdings in relation to only other top 10 stocks and adds this
        as a feature to the dataframe of scraped results.
        Loads the resultant dataframe into Snowflake's STOCK_HOLDINGS table.
        """
        URL = "https://www.ssga.com/sg/en/institutional/etfs/funds/spdr-straits-times-index-etf-es3"
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")

        today = date.today()

        indexTopHoldings = soup.find_all("div", class_="index-top-holdings")
        companies = []
        tickers = []
        true_weights = []
        f = indexTopHoldings[0]
        for c in f.find_all("td", class_="label"):
            companies.append(c.get_text())
        for t in f.find_all("td", class_="ticker"):
            ticker = str(t.get_text())
            ticker = ticker[:-3] + ".SI"
            tickers.append(ticker)
        for w in f.find_all("td", class_="weight"):
            weight = round(float(w.get_text()[:-1])/100, 4)
            true_weights.append(weight)
        top_10_weights = HoldingsScraper.calcReweight(true_weights)
        df =  pd.DataFrame({'COMPANY': companies, 'TICKER': tickers, 'TRUE_WEIGHT':true_weights, 'TOP_10_WEIGHT':top_10_weights})
        df['date'] = today

        insert_data(df, "IS3107_STOCKS_DATA", "STOCKS_DATA", "STOCK_HOLDINGS" )

    @staticmethod
    def calcReweight(true_weights):
        """
        Calcualtes the weight of each stock in true_weights in relation only to other stocks in true_weights,
        creating a weightage that will add up to 100%.

        Parameters
        ----------
        true_weights: Array of weights of each stock

        Returns
        -------
        top_10_weights: Array of weights of each stock in true_weights in relation to other stocks in true_weights
        """
        totalWeight = sum(true_weights)
        top_10_weights = []
        for i in true_weights:
            new_weight = round(i/totalWeight, 4)
            top_10_weights.append(new_weight)
        return top_10_weights


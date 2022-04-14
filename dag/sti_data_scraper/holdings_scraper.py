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

        df = df.head(2)

        insert_data(df, "IS3107_STOCKS_DATA", "STOCKS_DATA", "STOCK_HOLDINGS" )

    @staticmethod
    def calcReweight(true_weights):
        totalWeight = sum(true_weights)
        top_10_weights = []
        for i in true_weights:
            new_weight = round(i/totalWeight, 4)
            top_10_weights.append(new_weight)
        return top_10_weights


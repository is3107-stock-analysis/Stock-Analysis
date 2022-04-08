import requests
from bs4 import BeautifulSoup
import pandas as pd

class HoldingsScraper:

    @staticmethod
    def scrape_holdings():
        URL = "https://www.ssga.com/sg/en/institutional/etfs/funds/spdr-straits-times-index-etf-es3"
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")
        
        fundTopHoldings = soup.find_all("div", class_="fund-top-holdings")
        companies = []
        tickers = []
        true_weights = []
        f = fundTopHoldings[0]
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
        df1 =  pd.DataFrame({'company': companies, 'ticker': tickers, 'true_weights':true_weights, 'top_10_weights':top_10_weights})
        df1.to_csv("fund_top_holdings.csv", index=False)
        
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
        df2 =  pd.DataFrame({'company': companies, 'ticker': tickers, 'true_weights':true_weights, 'top_10_weights':top_10_weights})
        df2.to_csv("index_top_holdings.csv", index=False)

        return [df1, df2]

    @staticmethod
    def calcReweight(true_weights):
        totalWeight = sum(true_weights)
        top_10_weights = []
        for i in true_weights:
            new_weight = round(i/totalWeight, 4)
            top_10_weights.append(new_weight)
        return top_10_weights


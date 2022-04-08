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
        weights = []
        f = fundTopHoldings[0]
        for c in f.find_all("td", class_="label"):
            companies.append(c.get_text())
        for t in f.find_all("td", class_="ticker"):
            tickers.append(t.get_text())
        for w in f.find_all("td", class_="weight"):
            weights.append(w.get_text())
        df1 =  pd.DataFrame({'company': companies, 'ticker': tickers, 'weight':weights})
        df1.to_csv("fundTopHoldings.csv", index=False)
        
        indexTopHoldings = soup.find_all("div", class_="index-top-holdings")
        companies = []
        tickers = []
        weights = []
        f = indexTopHoldings[0]
        for c in f.find_all("td", class_="label"):
            companies.append(c.get_text())
        for t in f.find_all("td", class_="ticker"):
            tickers.append(t.get_text())
        for w in f.find_all("td", class_="weight"):
            weights.append(w.get_text())
        df2 =  pd.DataFrame({'company': companies, 'ticker': tickers, 'weight':weights})
        df2.to_csv("indexTopHoldings.csv", index=False)

        return [df1, df2]


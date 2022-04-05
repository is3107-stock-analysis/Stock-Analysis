from duckduckgo_search import ddg_news
import pandas as pd
import pandas as pd
from time import sleep

class NewsScraper:

    @classmethod
    def scrape_tickers(cls, tickers, company_name):
        df = pd.DataFrame(columns=['title', 'date', 'body', 'url'])
        for k in range(len(tickers)):
          keywords = company_name[k]
          r = ddg_news(keywords, region='wt-wt', safesearch='Off', max_results=10)
          results = pd.DataFrame(r)
          results = results[['title', 'date', 'body', 'url']]
          results['company'] = company_name[k]
          df = pd.concat([df, results], ignore_index=True)
          sleep(5)

        return df

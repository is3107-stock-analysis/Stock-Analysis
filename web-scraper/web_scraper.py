from GoogleNews import GoogleNews
import pandas as pd
from newspaper import Article
from newspaper import Config
import pandas as pd
import nltk
# config will allow us to access the specified url for which we are 
# not authorized. Sometimes we may get 403 client error while parsing 
# the link to download the article.

class NewsScraper:

    def __init__(self):
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
        config = Config()
        config.browser_user_agent = user_agent
        config.request_timeout = 10
        self.config = config

    def scrape_tickers(self, tickers):
        googlenews=GoogleNews(start='05/01/2021',end='04/31/2022')
        googlenews.search(tickers[0])
        result=googlenews.result()
        df=pd.DataFrame(result)
        print(df.head())
        for i in range(2,20):
            googlenews.getpage(i)
            result=googlenews.result()
            df=pd.DataFrame(result)

        df.to_excel("articles.xlsx")


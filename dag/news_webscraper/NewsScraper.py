from GoogleNews import GoogleNews
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
from time import sleep

class NewsScraper:

  def scrape_news(tickers, companies):
    today = date.today().strftime("%m/%d/%Y")
    three_months_ago_d = date.today() + relativedelta(months=-3)
    three_months_ago = three_months_ago_d.strftime("%m/%d/%Y")
    two_months_ago_d = date.today() + relativedelta(months=-2)
    two_months_ago = two_months_ago_d.strftime("%m/%d/%Y")
    one_months_ago_d = date.today() + relativedelta(months=-1)
    one_months_ago = one_months_ago_d.strftime("%m/%d/%Y")

    df = pd.DataFrame(columns=['title', 'datetime', 'link', 'desc', 'company', 'ticker'])
    googlenews = GoogleNews()
    googlenews.set_lang('en')

    for i in range(len(companies)):
      googlenews.set_time_range(one_months_ago, today)
      googlenews.search(companies[i])
      sleep(6)
      googlenews.set_time_range(two_months_ago, one_months_ago)
      googlenews.search(companies[i])
      sleep(6)
      googlenews.set_time_range(three_months_ago, two_months_ago)
      googlenews.search(companies[i])

      r = googlenews.results()
      results = pd.DataFrame(r) 
      results = results[['title', 'datetime', 'link', 'desc']]
      results['company'] = companies[i]
      results['ticker'] = tickers[i]
      df = pd.concat([df, results], ignore_index=True)
      googlenews.clear()
      
    df.to_csv("news_data.csv", index=False)
    return df

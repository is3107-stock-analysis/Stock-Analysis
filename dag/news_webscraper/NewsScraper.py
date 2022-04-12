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
    #three_months_ago_minus_one_day_d = three_months_ago_d + relativedelta(days=-1)
    #three_months_ago_minus_one_day = three_months_ago_minus_one_day_d.strftime("%m/%d/%Y")

    two_months_ago_d = date.today() + relativedelta(months=-2)
    two_months_ago = two_months_ago_d.strftime("%m/%d/%Y")
    two_months_ago_minus_one_day_d = two_months_ago_d + relativedelta(days=-1)
    two_months_ago_minus_one_day = two_months_ago_minus_one_day_d.strftime("%m/%d/%Y")

    one_months_ago_d = date.today() + relativedelta(months=-1)
    one_months_ago = one_months_ago_d.strftime("%m/%d/%Y")
    one_months_ago_minus_one_day_d = one_months_ago_d + relativedelta(days=-1)
    one_months_ago_minus_one_day = one_months_ago_minus_one_day_d.strftime("%m/%d/%Y")

    #quarter = str(date.today().year) + " Q" + str((date.today().month-1)//3 + 1)

    df = pd.DataFrame(columns=['title', 'datetime', 'link', 'company', 'ticker'])
    googlenews = GoogleNews()
    googlenews.set_lang('en')

    for i in range(len(companies)):
      df = NewsScraper.get_company_news(googlenews, df, one_months_ago_d, one_months_ago, today, companies[i], tickers[i])

      df = NewsScraper.get_company_news(googlenews, df, two_months_ago_d, two_months_ago, one_months_ago_minus_one_day, companies[i], tickers[i])
      
      df = NewsScraper.get_company_news(googlenews, df, three_months_ago_d, three_months_ago, two_months_ago_minus_one_day, companies[i], tickers[i])
    #print(df.dtypes)
    #print(df.head())
    return df

  def get_company_news(googlenews, df, start_d, start, end, company, ticker):
    googlenews.clear()
    googlenews.set_time_range(start, end)
    googlenews.search(company)
    sleep(12)

    r = googlenews.results()
    results = pd.DataFrame(r) 
    results = results[['title', 'datetime', 'link']]
    results['company'] = company
    results['ticker'] = ticker
    results.datetime.fillna(pd.to_datetime(start_d), inplace=True) #replace NA with the starting date
    df = pd.concat([df, results], ignore_index=True)
    
    googlenews.clear()

    return df

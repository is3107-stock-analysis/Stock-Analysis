import sys, os
sys.path.append(os.path.abspath(os.path.join('..', 'sql_helpers')))

from GoogleNews import GoogleNews
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
from time import sleep
import contractions

from sql_helpers.sql_query import query_table
from sql_helpers.sql_upload import insert_data

class NewsScraper:

  def scrape_news():
    '''
    Scrapes the news from the current date up to one month ago.
    Uses Google News.
    '''
    today = date.today().strftime("%m/%d/%Y")

    one_months_ago_d = date.today() + relativedelta(months=-1)
    one_months_ago = one_months_ago_d.strftime("%m/%d/%Y")
    one_months_ago_minus_one_day_d = one_months_ago_d + relativedelta(days=-1)
    one_months_ago_minus_one_day = one_months_ago_minus_one_day_d.strftime("%m/%d/%Y")

    #quarter = str(date.today().year) + " Q" + str((date.today().month-1)//3 + 1)

    df = pd.DataFrame(columns=['title', 'datetime', 'link', 'company', 'ticker'])
    googlenews = GoogleNews()
    googlenews.set_lang('en')

    
    holdings = query_table("IS3107_STOCKS_DATA", "STOCKS_DATA", "STOCK_HOLDINGS", one_months_ago_d, date.today())
    companies = list(holdings.COMPANY)
    tickers = list(holdings.TICKER)


    for i in range(len(companies)):
      df = NewsScraper.get_company_news(googlenews, df, one_months_ago_d, one_months_ago, today, companies[i], tickers[i])

    df_cleaned = NewsScraper.start_clean(df)

    insert_data(df_cleaned, "IS3107_NEWS_DATA", "NEWS_DATA", "NEWS_TABLE")

  def get_company_news(googlenews, df, start_d, start, end, company, ticker):
    '''
    Starts a query for the news data with the specifed company name and ticker.

    A timer to make the scraper sleep is used to ensure that the code does not 
    get blocked by Google for sending too many requests at once. 
    '''
    googlenews.clear()
    googlenews.set_time_range(start, end)
    googlenews.search(company)
    sleep(8)

    r = googlenews.results()
    results = pd.DataFrame(r) 
    results = results[['title', 'datetime', 'link']]
    results['company'] = company
    results['ticker'] = ticker
    results.datetime.fillna(pd.to_datetime(start_d), inplace=True) #replace NA with the starting date
    df = pd.concat([df, results], ignore_index=True)
    
    googlenews.clear()

    return df

  def start_clean(input_df_news):
    '''
    Part of the Transformation process to help clean scraped news data.
    Through Exploratory Data Analysis, we found a few key issues relating to the scraped news
      1. Duplicated titles and URLs were found in the scraped news
      2. Contractions caused errors when pushing the string into Snowflake due to the aprostrophe 
      attached
      3. Open inverted commas also posed as an issue when loading strings into Snowflake
      4. Ellipses in certain scraped titles may pose as an issue for Sentiment Analysis

      ----------------
      Parameters
      input_df_news: Dataframe of the scraped news data
    '''
    # Remove duplicates
    df_news = NewsScraper.removeDuplicates(input_df_news)

    # Expand contractions
    df_news['title'] = df_news['title'].apply(lambda x: contractions.fix(x))

    # Remove any open inverted commas 
    df_news['title'] = df_news['title'].apply(lambda x: x.replace('"',''))

    # Remove any aprostrophe
    df_news['title'] = df_news['title'].apply(lambda x: x.replace("'",''))

    # Remove trailing ellipse
    df_news['title'] = df_news['title'].apply(lambda x: x.replace('...',''))
    
    # Reset the index due to the dropping of duplicates
    df_news.reset_index()
    
    return df_news

  def removeDuplicates(news_data):
    '''
    Removes duplicates in the title and link.
    Only the latest instance of the news article is kept and the rest are dropped.
    
    ----------------
    Parameters
    news_data: Dataframe of the scraped news data
    '''
    no_dupes_df = news_data.sort_values('datetime').drop_duplicates(subset = ['title','link'], keep='last')
    no_dupes_df.reset_index()
    return no_dupes_df


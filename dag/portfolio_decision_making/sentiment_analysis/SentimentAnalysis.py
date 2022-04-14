import sys, os
sys.path.append(os.path.abspath(os.path.join('..', 'sql_helpers')))

from sql_helpers.sql_query import query_table
from sql_helpers.sql_upload import insert_data

import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import date
from dateutil.relativedelta import relativedelta

class SentimentAnalysis:

    def get_sentiments(ti):
        """
        Handles the sentiment analysis and combines the results with results from suggested_reweighting. 
        Queries Snowflake for News data to conduct sentiment analysis. 
        Pulls suggested_reweighting dataframe from XComs.
        Loads resultant merged results into REWEIGHTING table in Snowflake.

        Parameters
        ----------
        companies: array of companies to be analysed
        headlines_df: dataframe of news headlines from these companies
        """
        today = date.today()
        one_months_ago_d = date.today() + relativedelta(months=-1)
        headlines_df = query_table("IS3107_NEWS_DATA", "NEWS_DATA", "NEWS_TABLE", one_months_ago_d, today)

        model = SentimentIntensityAnalyzer()
        sentiment_predictions = SentimentAnalysis.getPredictions(model, headlines_df)

        optimized_df = pd.read_json(ti.xcom_pull(key="reweighting", task_ids=["suggest_reweight"])[0])

        final_results_df = pd.merge(optimized_df, sentiment_predictions, on='Ticker')

        #add concordance column
        concordance = []
        for index, row in final_results_df.iterrows():
            if row['Adjustment'] > 0 and row['SENTIMENT'] == "negative":
                concordance.append(False)
            elif row['Adjustment'] < 0 and row['SENTIMENT'] == "positive":
                concordance.append(False)
            else:
                concordance.append(True)
        final_results_df['CONCORDANCE'] = concordance
        final_results_df['DATE'] = str(date.today())

        insert_data(final_results_df, "IS3107_RESULTS", "FINAL_OUTPUT", "REWEIGHTING")


    """
    Iterates through a given dataframe of news data and aggregates sentiments for each ticker in the dataframe

    Parameters
    ----------
    model: model to be used to analyse sentiments
    headlines_df: dataframe of news data to be analysed

    Returns
    -------
    df : dataframe of tickers along with their aggregated sentiments across news pertaining to their company
    """
    def getPredictions(model, headlines_df):
        sentiment_pred = []
        tickers = headlines_df['TICKER'].unique()

        for ticker in tickers:
            df = headlines_df.loc[headlines_df['TICKER'] == ticker]
            headlines_arr = df['TITLE']
            total_polarity = 0
            sentiment = 'neutral'
            for h in headlines_arr:
                polarity = model.polarity_scores(h)['compound']
                total_polarity += polarity
            if total_polarity > 0.2:
                sentiment = 'positive'
            elif total_polarity < -0.2:
                sentiment = 'negative'
            sentiment_pred.append(sentiment)
        
        df = pd.DataFrame({'Ticker': tickers, 'SENTIMENT': sentiment_pred})
        return df


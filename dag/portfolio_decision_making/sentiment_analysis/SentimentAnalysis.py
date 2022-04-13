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
        Handles the sentiment analysis process

        Parameters
        ----------
        companies: array of companies to be analysed
        headlines_df: dataframe of news headlines from these companies
        """
        today = date.today()
        three_months_ago_d = date.today() + relativedelta(months=-3)
        headlines_df = query_table("", "", "", three_months_ago_d, today)

        model = SentimentIntensityAnalyzer()
        sentiment_predictions = SentimentAnalysis.getPredictions(model, headlines_df)
        optimized_df = pd.read_json(ti.xcom_pull(key="reweighting", task_ids=["suggest_reweight"])[0])

        final_results_df = pd.merge(sentiment_predictions, optimized_df, on='TICKER')

        #add concordance column
        concordance = []
        for index, row in final_results_df.iterrows():
            if row['REWEIGHTING'] > 0 and row['SENTIMENT'] == "negative":
                concordance.append(False)
            elif row['REWEIGHTING'] < 0 and row['SENTIMENT'] == "positive":
                concordance.append(False)
            else:
                concordance.append(True)
        final_results_df['CONCORDANCE'] = concordance
        final_results_df['DATE'] = str(date.today())
        insert_data(final_results_df, "IS3107_RESULTS", "FINAL_OUTPUT", "REWEIGHTING")


    def getPredictions(model, headlines_df):
        sentiment_pred = []
        tickers = headlines_df['TICKER'].unique()

        for ticker in tickers:
            df = headlines_df.loc[headlines_df['TICKER'] == ticker]
            headlines_arr = df['title']
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
        
        df = pd.DataFrame({'TICKER': tickers, 'SENTIMENT': sentiment_pred})
        return sentiment_pred


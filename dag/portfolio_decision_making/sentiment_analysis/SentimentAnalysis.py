from numpy import positive
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import date

class SentimentAnalysis:

    def get_sentiments(ti, headlines_df):
        """
        Handles the sentiment analysis process

        Parameters
        ----------
        companies: array of companies to be analysed
        headlines_df: dataframe of news headlines from these companies
        """
        model = SentimentAnalysis.get_vader()
        sentiment_predictions = SentimentAnalysis.getPredictions(model, headlines_df)
        optimized_df = pd.read_json(ti.xcom_pull(key="reweighting", task_ids=["suggest_reweight"])[0])

        returns_df = pd.merge(sentiment_predictions, optimized_df, on='TICKER')

        ### Push into XCOM 
        #ti.xcom_push(key="decision_making_df", value=returns_df.to_json())

        return returns_df.to_json() 

    def get_vader():
        sid = SentimentIntensityAnalyzer()
        return sid

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
        df['DATE'] = str(date.today())
        return df


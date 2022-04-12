from numpy import positive
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class SentimentAnalysis:

    def get_sentiments(companies, headlines_df):
        """
        Handles the sentiment analysis process

        Parameters
        ----------
        companies: array of companies to be analysed
        headlines_df: dataframe of news headlines from these companies
        """
        model = SentimentAnalysis.get_vader()
        sentiment_results = SentimentAnalysis.getPredictions(model, companies, headlines_df)
        return sentiment_results

    def get_vader():
        #finance_sentiments_df = pd.read_csv("LoughranMcDonald_MasterDictionary_2020.csv")
        #finance_sentiments = finance_sentiments_df[['Word', 'Negative', 'Positive']]
        
        #negatives = finance_sentiments.loc[finance_sentiments['Negative'] > 0]['Word']
        #positives =  finance_sentiments.loc[finance_sentiments['Positive'] > 0]['Word']
        #neutrals = finance_sentiments.loc[(finance_sentiments['Positive'] <=0) & (finance_sentiments['Negative'] <=0)]['Word']

        sid = SentimentIntensityAnalyzer()
        #sid_lex = sid.lexicon
        #sid_lex.update({word:-2.0 for word in negatives})
        #sid_lex.update({word:2.0 for word in positives})
        #sid_lex.update({word:0 for word in neutrals})

        return sid

    def getPredictions(model, companies, headlines_df):
        sentiment_pred = []

        for company in companies:
            df = headlines_df.loc[headlines_df['company'] == company]
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
        print(sentiment_pred)
        return pd.DataFrame({'company': companies, 'sentiment': sentiment_pred})


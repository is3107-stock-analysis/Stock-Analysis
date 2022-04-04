import pandas as pd
import nltk
import random
#from finbert import FinBERT
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

class SentimentAnalysis:

    @classmethod
    def sentiment_analysis_pipeline(cls, headlines_df: pd.DataFrame):
        """
        Handles the sentiment analysis process

        Parameters
        ----------
        headlines_df: dataframe of news headlines
        """
        model = cls.get_updated_vader()
        sentiment_results = cls.getPredictions(model, headlines_df)
        return sentiment_results

    @classmethod
    def get_updated_vader(cls):
        finance_sentiments_df = pd.read_csv("LoughranMcDonald_MasterDictionary_2020.csv")
        finance_sentiments = finance_sentiments_df[['Word', 'Negative', 'Positive']]
        
        negatives = finance_sentiments.loc[finance_sentiments['Negative'] > 0]['Word']
        positives =  finance_sentiments.loc[finance_sentiments['Positive'] > 0]['Word']
        neutrals = finance_sentiments.loc[(finance_sentiments['Positive'] <=0) & (finance_sentiments['Negative'] <=0)]['Word']

        sid = SentimentIntensityAnalyzer()
        sid_lex = sid.lexicon
        sid_lex.update({word:-2.0 for word in negatives})
        sid_lex.update({word:2.0 for word in positives})
        sid_lex.update({word:0 for word in neutrals})

        return sid

    @classmethod
    def getPredictions(cls, model, headlines_df):
        sentiment_pred = []
        headlines_arr = headlines_df.loc['headlines']


        for h in headlines_arr:
            polarity_og = model.polarity_scores(h)['compound']
            #polarity_updated = sid_updated.polarity_scores(sentence)['compound']

            if polarity_og >= 0.05:
                sentiment_pred.append("positive")
            elif polarity_og <= -.05:
                sentiment_pred.append("negative")
            else:
                sentiment_pred.append("neutral")

        return {"headline": headlines_arr, "sentiment": sentiment_pred}


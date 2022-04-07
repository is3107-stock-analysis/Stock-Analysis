from langdetect import detect
from itranslate import itranslate as itrans

class DataCleaning:

    def generate_data_cleaned(self, news_firm):
        """
        Conducts data cleaning process
        Fill in null values -> replace wrong articles scraped -> drop duplicated data by Company
        -> translate articles that are in different language -> drop articles that are less than 15 words
        """
        # Drop null values
        news_data_filled = news_firm.apply(self.fillNaValues, axis=1)
        news_data_filled = news_data_filled.drop(news_data_filled[(news_data_filled.Summary == 'nan') |
                                                                (news_data_filled.Article == 'nan')].index).reset_index(
            drop=True)

        # Drop duplicated data by Company
        news_data_filled_corrected = self.removeDuplicates(news_data_filled)

        # Translate articles (Not needed, see what we scrape.)
        print("Translating languages")
        news_data_filled_corrected_2 = self.translateLanguage(news_data_filled_corrected)

        # Drop articles that are less than 15 words (Not needed, see what we scrape.)
        print("Dropping articles w/ less than 15 words")
        less_idx = news_data_filled_corrected_2[
            news_data_filled_corrected_2['Article'].apply(lambda x: len(x.split()) <= 15)].index
        news_data_filled_corrected_4 = news_data_filled_corrected_2.drop(less_idx).reset_index(drop=True)

        return news_data_filled_corrected_4

    # def returnNonNaValues(self, row):
    #     '''
    #     Handles null values in the news data set. Null values occurs due to error in scraping or unable
    #     to identify the section to scrape the text.
    #     Non-null articles will be returned.
    #     '''

    #     row['Title'] = str(row['Title'])
    #     row['Summary'] = str(row['Summary'])
    #     row['Article'] = str(row['Article'])
    #     row['Body'] = str(row['Body'])

    #     if row['Title'] == 'nan' or row['Summary'] == 'nan' or row['Article'] == 'nan':
    #         return row

    def removeDuplicates(self, news_data):
        '''
        1. Drops originally duplicated data in each Company
        2. Drops duplicated articles scraped in each Company. Duplicates occurs due to expired links.
        eg. 5 different news articles from CNN. When the link expires, it will direct to the homepage, hence all 5 links will
        return content of the homepage.
        '''
        data = news_data.drop_duplicates(subset=['Headline', 'Company'], keep='first').reset_index(drop=True)

        dupli_article_idx = data[data.duplicated(subset=['Article', 'Company'])].index
        data.iloc[dupli_article_idx.values] = data.iloc[dupli_article_idx.values].apply(self.replaceScrapedWithBody, axis=1)
        data_2 = data.drop_duplicates(subset=['Article', 'Company'], keep='first')
        data_3 = data_2.drop(data_2[data_2.Article == 'nan'].index).reset_index(drop=True)

        return data_3

    def detectLang(self, text):
        '''Detects language of individual articles'''
        try:
            lang = detect(text)
        except:
            lang = "error"

        return lang

    def translateLanguage(self, news_data):
        '''Detects language of all articles and translate articles that are in foreign language'''

        detectedLang = news_data['Article'].apply(lambda x: self.detectLang(x))
        nonEng_idx = detectedLang[detectedLang != 'en'].index
        news_data.loc[nonEng_idx, 'Article'] = news_data.loc[nonEng_idx, 'Article'].apply(
            lambda text: itrans(text, to_lang="en"))

        return news_data

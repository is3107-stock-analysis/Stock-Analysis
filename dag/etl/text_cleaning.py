import re
import string

import emoji
import nltk
import regex
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import RegexpTokenizer

nltk.download('punkt')
nltk.download('wordnet')
nltk.download('stopwords')
cachedStopWords = stopwords.words("english")

lemmatizer = WordNetLemmatizer()
punct = set(string.punctuation)
tokenizer = RegexpTokenizer(r'\w+')


class TextCleaning:
    def generate_text_cleaned(self, data_cleaned_df):
        # Textual cleaning
        # TODO: save these as columns: noUnicode, noStopwords, Date (Change to 1st date of the month), 
        # Region, Links, Title, Article, Industry, Parent Country
        # lower case
        print("Making all text lowercase")
        data_cleaned_df['Article_lower'] = data_cleaned_df['Article'].apply(lambda x: x.lower())

        # decontract 
        print("Decontracting Contractions")
        data_cleaned_df['Article_decontracted'] = data_cleaned_df["Article_lower"].apply(lambda x: self.decontracted(x))
        data_cleaned_df['Article_noUnwanted'] = self.remove_unwanted_words(data_cleaned_df['Article_decontracted'])

        # Removes all the texts with emojis in it
        print("Removing fields with emojis")
        to_remove = self.remove_emoji_sent(
            data_cleaned_df['Article_noUnwanted'])  # Returns a list of indexes to drop from df
        data_cleaned_df = data_cleaned_df.drop(to_remove)
        data_cleaned_df = data_cleaned_df.reset_index(drop=True)

        # Removes all \n and unicode values from remaining articles
        print("Removing unicode and newline")
        data_cleaned_df['Article_noUnicode'] = self.remove_unicode_newline(data_cleaned_df['Article_noUnwanted'])

        # Remove stopwords
        print("Removes stopwords")
        data_cleaned_df['Article_noStop'] = data_cleaned_df['Article_noUnicode'].apply(
            lambda x: self.remove_stopwords(x))

        # Drop empty fields
        print("Dropping fields that are empty")
        data_cleaned_df.drop(index=self.drop_empty_articles(data_cleaned_df['Article_noStop']), inplace=True)
        data_cleaned_df = data_cleaned_df.reset_index(drop=True)

        # Convert date to 1 day of month
        data_cleaned_df["Timestamp"] = data_cleaned_df["Published Time"].astype('datetime64[M]')

        return data_cleaned_df

    def decontracted(self, phrase):
        '''Changes all contractions to full words'''

        # specific
        phrase = re.sub(r"won\'t", "will not", phrase)
        phrase = re.sub(r"can\'t", "can not", phrase)
        phrase = re.sub(r"\Acovid-19|\Acovid19", "covid 19", phrase)

        # general
        phrase = re.sub(r"n\'t", " not", phrase)
        phrase = re.sub(r"\'re", " are", phrase)
        phrase = re.sub(r"\'s", " is", phrase)
        phrase = re.sub(r"\’s", " is", phrase)
        phrase = re.sub(r"\'d", " would", phrase)
        phrase = re.sub(r"\'ll", " will", phrase)
        phrase = re.sub(r"\'t", " not", phrase)
        phrase = re.sub(r"\'ve", " have", phrase)
        phrase = re.sub(r"\'m", " am", phrase)

        # double space
        phrase = re.sub(r"\  ", " ", phrase)

        return phrase

    def remove_unwanted_words(self, article_column):
        '''Removes all unwanted words from list below and all links and digits'''

        # words
        unwanted_words = ['q1', 'q2', 'q3', 'q4', 'yoy', 'qoq', 'inc.']

        # links
        article_column = article_column.apply(lambda phrase: re.sub(r'http\S+', '', phrase))

        # digits
        article_column = article_column.apply(lambda phrase: re.sub(r"\d+\.\d+", "", phrase))
        article_column = article_column.apply(lambda phrase: re.sub(r"\d", "", phrase))
        article_column = article_column.apply(lambda phrase: re.sub(r"\$", "", phrase))

        article_column_cleaned = article_column.apply(
            lambda x: ' '.join([word for word in x.split() if word not in (unwanted_words)]))

        # double space
        article_column_cleaned = article_column_cleaned.apply(lambda phrase: re.sub(r"\  ", " ", phrase))

        return article_column_cleaned

    def remove_emoji_sent(self, article_column):
        '''Churns out a list of indexes which represents the rows to be deleted from the dataframe'''
        dropping_list = []
        for article_index in range(len(article_column)):
            data = regex.findall(r'\X', article_column[article_index])
            for word in data:
                if any(char in emoji.UNICODE_EMOJI['en'] for char in word):
                    dropping_list.append(article_index)
        return list(set(dropping_list))

    def remove_unicode_newline(self, article_column):
        '''
        Removes all unicode and newline values in each article
        '''
        article_column = article_column.apply(lambda para: para.replace("\n", " "))  # remove \n
        article_column = article_column.apply(lambda line: line.encode("ascii",
                                                                       "ignore"))  # Encode and decode to remove unicode, i.e. everything like \uf075 (This is removing emojis at the same time...)
        article_column = article_column.apply(lambda encoded: encoded.decode())
        return article_column

    def uncaught_punc(self, article_column):
        '''
        Returns a list of all the punctuations that were not caught by String's punctuation list
        '''
        outPunc = []
        hyphen_present = False
        for articleNum in range(len(article_column)):
            # checking whether the string contains punctuation.
            for i in article_column[articleNum]:
                if i == '-':
                    hyphen_present = True
                    continue
                if (i.isalnum() != True) and (i != ' ') and (i not in outPunc):
                    outPunc.append(i)
        if hyphen_present == True:
            outPunc.append('-')
        return outPunc

    def tokenize_sent(self, cleaned_articles):
        '''
        This tokenizes all the articles inputted into sentences
        '''
        token_sent = []
        for sent in cleaned_articles:
            sent = sent.replace("_", " ")
            token_sent.append(tokenizer.tokenize(sent))
        return token_sent

    def punc_removal(self, token_sent):
        '''
        Input is an array of arrays (token_sent)
        The output is a list of words from a sentence, which has no punctuation present
        '''
        # Want to loop through this array, then check if the token is a punctuation
        # If it is a punctuation, skip it. Retain those that are only words
        # so i need two empty lists. One in for loop and one outside
        list_sent = []
        for sentence in token_sent:
            sent_nopunc = []
            for word in sentence:
                if word not in punct:
                    sent_nopunc.append(word)
            list_sent.append(sent_nopunc)
        return list_sent

    def remove_stopwords(self, article):
        '''
        Takes in a sentence (string) to remove all stopwords
        '''
        text = ' '.join([word for word in article.split() if word not in cachedStopWords])
        return text

    def drop_empty_articles(self, articles):
        '''
        Returns a list of indexes to be dropped from df
        '''
        drop_row = []
        for text_ind in range(len(articles)):
            text = articles[text_ind]
            if text == "" or text == False:
                drop_row.append(text_ind)
        return drop_row

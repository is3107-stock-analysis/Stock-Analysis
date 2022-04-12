class DataCleaning:
    
    def __init__(self):
        pass

    def start_clean(self, news_csv):
        # Make the CSV into df
        df_news = pd.read_csv("news_data.csv")

        # Remove duplicates
        df_news2 = removeDuplicates(df_news)

        # Remove newline
        df_news2['title_no_newline'] = df_news2['title'].apply(lambda x: x.strip())
        # Remove trailing ellipse
        df_news2['title_no_newline_ellipse'] = df_news2['title_no_newline'].replace('\.+','.',regex=True)


    def removeDuplicates(self, news_data):
        no_dupes_df = news_data.sort_values('datetime').drop_duplicates(subset = ['title','link'], keep='last')
        no_dupes_df.reset_index()
        return no_dupes_df


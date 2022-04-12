class DataCleaning:

    @staticmethod
    def start_clean(df_news):
        # Remove duplicates
        df_news2 = DataCleaning.removeDuplicates(df_news)

        # Remove newline
        df_news2['title_no_newline'] = df_news2['title'].apply(lambda x: x.strip())
        # Remove trailing ellipse
        df_news2['title_no_newline_ellipse'] = df_news2['title_no_newline'].replace('\.+','.',regex=True)
        df_news2.drop(['title_no_newline'], axis = 1)
        df_news2.reset_index()
        
        return df_news2

    def removeDuplicates(news_data):
        no_dupes_df = news_data.sort_values('datetime').drop_duplicates(subset = ['title','link'], keep='last')
        no_dupes_df.reset_index()
        return no_dupes_df


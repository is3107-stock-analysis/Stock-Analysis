class DataCleaning:

    @staticmethod
    def start_clean(df_news):
        # Remove duplicates
        df_news2 = DataCleaning.removeDuplicates(df_news)
        # Remove trailing ellipse
        df_news2['title_no_ellipse'] = df_news2['title'].replace('\.+','.',regex=True)
        # Reset the index due to the dropping of duplicates
        df_news2.reset_index()
        
        return df_news2

    def removeDuplicates(news_data):
        no_dupes_df = news_data.sort_values('datetime').drop_duplicates(subset = ['title','link'], keep='last')
        no_dupes_df.reset_index()
        return no_dupes_df


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from news_webscraper.NewsScraper import NewsScraper
from sti_data_scraper.get_stock_data import get_data_for_multiple_stocks
from portfolio_decision_making.portfolio_optimization.optimization import get_optimized_portfolio
from portfolio_decision_making.portfolio_optimization.comparison_statistics import get_comparison_statistics

def helloWorld():
    test = NewsScraper()
    print("newsscraper success")
    print("Hello World")

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:

        """
        Scrape all required data
        """

        task1 = PythonOperator(
        task_id="hello_world",
        python_callable=helloWorld)

        get_stocks = PythonOperator(
        task_id="scrape_stocks_data",
        python_callable=get_data_for_multiple_stocks, 
        op_kwargs={"start_date":"2022-01-01", "end_date":"2022-04-01"}
        )

        """
        Perform Transformation Tasks
        """
        ## NEED to do cleaning of stock/news data first, then push into DB


        """
        Load into dataa warehouse
        """


        """
        Portfolio Analysis Section
        """

        #NEED to load stock data here

        #we minimize risk with while placing more emphasis on returns
        get_optimized_portfolio= PythonOperator(
        task_id="optimize_portfolio",
        python_callable=get_optimized_portfolio, 
        op_kwargs={"returns_scale":0.0001}
        )

        #Get the optimized portfolio statistics
        get_comparison_statistics = PythonOperator(
            task_id="get_statistics", 
            python_callable=get_comparison_statistics
        )

    
task1>>get_stocks>>get_optimized_portfolio>>get_comparison_statistics
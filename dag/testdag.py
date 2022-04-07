from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from webscraper.web_scraper import NewsScraper
from sti_data.get_stock_data import get_data_for_multiple_stocks

def helloWorld():
    test = NewsScraper()
    print("newsscraper success")
    print("Hello World")

with DAG(dag_id="hello_world_dag",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:


        task1 = PythonOperator(
        task_id="hello_world",
        python_callable=helloWorld)

        get_stocks = PythonOperator(
        task_id="stock",
        python_callable=get_data_for_multiple_stocks, 
        op_kwargs={"start_date":"1/1/2022", "end_date":"1/2/2022"}
        )

    
task1>>get_stocks
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from webscraper.web_scraper import NewsScraper


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

    
task1
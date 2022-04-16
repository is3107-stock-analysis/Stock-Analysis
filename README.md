# IS3107 Group Project

## Installation Steps:
1) create LinuxVMServer using import of Joel's download
2) ssh into VM (ssh -p 2222 airflow@localhost)
3) open a terminal in VM through VSS and git clone our repo
4) pip install virtualenv 
5) python3 -m venv {name}
6) source {name}/bin/activate
7) follow joel's tutorial 3 slide 12 to download airflow in the venv created 
8) airflow db init
9) create user account 
10) IMPT step: go to /home/airflow/airflow.cfg and change dags_folder location to our repo Stock-Analysis/dag AND change load_examples = False 
11) pip install -r requirements.txt (installs stuff like GoogleNews, pandas, newspaper3k for web scraper)
12) airflow scheduler 
13) open a new terminal, activate the venv again and run airflow webserver

## Running the DAG
Once the airflow webserver is running, open localhost:8080

Upon running the DAG on the webserver, the flow of the tasks being carried out will be as such:

1) insert_holdings
2) scrape_stocks_data and insert_news will be carried out in parallel to speed up the process
3) optimize_portfolio will be carried out after both processes in (2) is done
4) sugest_reweight
5) sentiment_analysis
6) get_statistics

Once the DAG is done running, the database/tables in Snowflake that we have connected the DAG to will be populated.

## Snowflake
To access our Snowflake:
https://ts39829.ap-southeast-1.snowflakecomputing.com

The credentials for accessing our Snowflake is included in the .env file which is also what we use to
establish the connection from our DAG to the tables in Snowflake.

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
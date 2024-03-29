import os
import requests
import pandas as pd
import urllib.parse

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def Data():
    url = "http://ccl.gachon.ac.kr/Data/Health/Mental_Health/SAFER/20240201/dumc_20240126/snuh_location.csv"
    data = pd.read_csv(url, sep='\t', encoding='utf-8')
    print(data.head())  # 데이터프레임 출력
    return data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 27),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'safer_data',
    default_args=default_args,
    description='safer_processing',
    schedule_interval=timedelta(days=1),
)

print_starting = PythonOperator(
    task_id='print_starting',
    python_callable=Data,
    provide_context=True,
    dag=dag,
)

print_starting

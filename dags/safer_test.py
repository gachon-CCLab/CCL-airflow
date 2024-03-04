import os
import requests
import pandas as pd
import urllib.parse

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta



def Data():
    url = "http://192.9.202.101:/디스크02/Data/Health/Mental_Health/SAFER/20240201/dumc_20240126/snuh_location.csv"
    data = pd.read_csv(url, sep='\t', encoding='utf-8')
    print(data.head())  # 데이터프레임 출력
    return data

# def Columns():
#     data = Data()  # 데이터 받아오기
#     data.columns = ['targetId', '이름', 'deviceId', '데이터', 'targetTime']  # 열 이름을 변경합니다.
#     return data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,2,27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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

# Column = PythonOperator(
#     task_id='Column',
#     python_callable=Columns,
#     provide_context=True,
#     dag=dag
# )

print_starting 

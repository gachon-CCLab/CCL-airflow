import os
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# FastAPI 엔드포인트에서 데이터를 가져오는 함수
def fetch_data_from_fastapi():
    url = "ccl.gachon.ac.kr:40001/rwd/crf/all"
    response = requests.get(url)
    data = response.json()
    return data

# 데이터를 평탄화하는 함수
def flatten_data(data):
    transformed_data = []
    for entry in data['data']:
        # 여기에 평탄화 로직 구현
        # 예시: flat_entry = { ... }
        # transformed_data.append(flat_entry)
        # ...

    return transformed_data

# 평탄화된 데이터를 DataFrame으로 변환하는 함수
def convert_to_dataframe(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')
    flattened_data = flatten_data(data)
    df = pd.DataFrame(flattened_data)
    # 여기에서 DataFrame을 저장하거나 다른 작업을 수행할 수 있습니다.

# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_processing',
    default_args=default_args,
    description='Fetch and process data from FastAPI',
    schedule_interval=timedelta(days=1),
)

# 작업 정의
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_from_fastapi,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=convert_to_dataframe,
    provide_context=True,
    dag=dag,
)

# 작업 순서 정의
fetch_data_task >> process_data_task

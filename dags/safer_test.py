import os
import requests
import pandas as pd
import urllib.parse

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta



    
 
def Data():
    url = "http://192.9.202.101:/volume2/디스크02/mnt/nas/disk02/Data/Health/Mental_Health/SAFER/20240201/snuh_20240126/snuh_sensing.csv"
    encoded_url = urllib.parse.quote(url, safe=':/')
    
    data = pd.read_csv(encoded_url, sep='\t', encoding='utf-8')
    return data

def Columns (data):
    
    Column_data =data.columns =['targetId','이름','deviceId','데이터','targetTime']

    return Column_data


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
    task_id = 'print_starting',
    python_callable = Data,
    provide_context = True,
    dag = dag,
)
Column = PythonOperator(
    task_id = 'Column',
    python_callable = Columns,
    provide_context = True,
    dag = dag
)

# t1 = FileSensor(
#     task_id = 'sensor_a',
#     fs_conn_id = 'file_sensor',
#     filepaht='snuh_sensing.csv',
#     dag=dag,
# )




print_starting >> Column
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def query_mysql():
    # SQLAlchemy 엔진을 사용하여 MySQL 연결 생성
    engine = create_engine('mysql+pymysql://root:root@210.102.181.208:40011/safer')
    with engine.connect() as connection:
        result = pd.read_sql_query('SELECT * FROM crf', con=connection)
    print(result)

with DAG('mysql_example_dag', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    query_task = PythonOperator(
        task_id='query_mysql',
        python_callable=query_mysql
    )

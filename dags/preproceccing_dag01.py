from airflow.models import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator


dag = DAG(
    dag_id="preprocessing_module",
    start_date=datetime(2023, 1, 12),
    schedule="@daily",
)

EmptyOperator(task_id="task", dag=dag)
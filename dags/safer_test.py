from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import ray
import pandas as pd
import json
from pandas import json_normalize

#1013JYS 행동 분석을 위험행동 시간에 센서가 어떻게 되는지 데이터 분석
#다른 환자데이터와 함께 분석하여 칼럼간의 상관관계 분석을 해도 좋을듯 

def load_txt_file(file_path):
    try:
        # 파일을 DataFrame으로 로드
        data = pd.read_csv(file_path, sep='\t', encoding='utf-8')
        return data
    except Exception as e:
        print(f"An error occurred while loading the file: {e}")
        return None
    
def modify(data):
    try:
        # 데이터 수정
        data.columns = ['targetId','이름','deviceId','데이터','targetTime']
        data = data.applymap(lambda x: x.strip(',') if isinstance(x, str) else x)
        data['데이터'] = data['데이터'].str.replace('""', '"')
        data['데이터'] = data['데이터'].str.replace('",', ',')
        return data
    except Exception as e:
        print(f"Data cannot be modified: {e}")
        return None

def json_to_pandas(data):
    try:
        # JSON 데이터를 파싱하여 새로운 열로 추가
        data['데이터'] = data['데이터'].apply(lambda x: json.loads(x))
        df_normalized = json_normalize(data['데이터'])
        # 기존 열 삭제 및 새로운 열 추가
        data = pd.concat([data.drop(['데이터'], axis=1), df_normalized], axis=1)
        return data
    except Exception as e:
        print(f"Failed to convert JSON to pandas: {e}")
        return None
    
class PatientData:
    def __init__(self, data, id):
        self.data = data
        self.id = id

    def get_patient_data(self):
        try:
            patient_data = self.data[(self.data['이름'] == self.id)]
            return patient_data
        except Exception as e:
            print(f"Failed to retrieve patient data: {e}")
            return None

def load_data(**kwargs):
    file_path = '/mnt/nas/disk02/Data/Health/Mental Health/SAFER/20240201/snuh_20240126/snuh_sensing.csv'
    return file_path

def process_data(file_path, **kwargs):
    data = load_txt_file(file_path)
    modified_data = modify(data)
    converted_data = json_to_pandas(modified_data)
    
    patient = PatientData(converted_data, "101-3JYS")
    patient_data = patient.get_patient_data()
    print(patient_data.head())
    
    patient_data.to_csv('101-3JYS.csv', index=False)
    
    ray.shutdown()

# Airflow DAG 설정
default_args ={
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024,2,18),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

dag = DAG(
    'data_processing_pipline',
    default_args=default_args,
    description='A DAG to process patient data',
    schedule_interval =timedelta(days=1), #DAG를 실행하는 주기 설정
)

# DAG 내의 작업 정의
load_data_task = PythonOperator(
    task_id ='load_data',
    python_callable = load_data,
    dag = dag,
)

process_data_task = PythonOperator(
    task_id = 'process_data',
    python_callable = process_data,
    op_kwargs={'file_path': '{{ task_instance.xcom_pull(task_ids="load_data") }}'},
    provide_context=True,
    dag = dag,
)

load_data_task >> process_data_task

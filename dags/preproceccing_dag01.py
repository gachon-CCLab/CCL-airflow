import os
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# FastAPI 엔드포인트에서 데이터를 가져오는 함수
def fetch_data_from_fastapi():
    url = "http://ccl.gachon.ac.kr:40001/rwd/crf/all"
    response = requests.get(url)
    data = response.json()
    return data

# 데이터를 평탄화하는 함수
def flatten_data(data):
    transformed_data = []
    for entry in data['data']:
        flat_entry = {
            "valve_model": entry.get("valve_model", ""),
            "valve_size": entry.get("valve_size", 0),
            "valve_type": entry.get("valve_type", 0),
        }

        # ClinicalData 추출
        clinical_data = entry.get('ClinicalData', {})
        flat_entry.update({
            "age": clinical_data.get('age'),
            "sex": str(clinical_data.get('sex')),
            "height": clinical_data.get('height'),
            "weight": clinical_data.get('weight'),
            "bmi": clinical_data.get('bmi'),
            "smoking": clinical_data.get('smoking'),
            "htn": clinical_data.get('htn'),
            "dm": clinical_data.get('dm'),
            "ckd": clinical_data.get('ckd')
        })

        # LabData 추출
        lab_data = entry.get('LabData', {})
        flat_entry.update({
            "hgb": lab_data.get('hgb'),
            "hba1c": lab_data.get('hba1c'),
            "ast": lab_data.get('ast'),
            "alt": lab_data.get('alt'),
            "creatine": lab_data.get('creatinine'),
            "nt_pro_bnp": lab_data.get('nt_pro_bnp')
        })

        # Vital 추출
        vital_data = entry.get('Vital', {})
        flat_entry.update({
            "sbp": vital_data.get('sbp'),
            "dbp": vital_data.get('dbp'),
            "pr": vital_data.get('pr'),
            "rr": vital_data.get('rr')
        })

        # PreCt 추출
        pre_ct_data = entry.get('PreCt', {})
        flat_entry.update({
            "lvot_perimeter": pre_ct_data.get('lvot_perimeter'),
            "lvot_diameter": pre_ct_data.get('lvot_diameter'),
            "sov_height_lcc": pre_ct_data.get('sov_height_lcc'),
            "sov_height_rcc": pre_ct_data.get('sov_height_rcc'),
            "sov_height_ncc": pre_ct_data.get('sov_height_ncc'),
            "sov_diameter_lcc": pre_ct_data.get('sov_diameter_lcc'),
            "sov_diameter_rcc": pre_ct_data.get('sov_diameter_rcc'),
            "sov_diameter_ncc": pre_ct_data.get('sov_diameter_ncc'),
            "stj_diameter_min": pre_ct_data.get('stj_diameter_min'),
            "stj_diameter_max": pre_ct_data.get('stj_diameter_max'),
            "coh_left": pre_ct_data.get('coh_left'),
            "coh_right": pre_ct_data.get('coh_right'),
            "annulus_area": pre_ct_data.get('annulus_area'),
            "annulus_diamet_min": pre_ct_data.get('annulus_diameter_min'),
            "annulus_diamet_max": pre_ct_data.get('annulus_diameter_max'),
            "annulus_perimeter": pre_ct_data.get('annulus_perimeter'),
            "aao_diameter_min": pre_ct_data.get('aao_diameter_min'),
            "aao_diameter_max": pre_ct_data.get('aao_diameter_max'),
            "cs_lcc": pre_ct_data.get('cs_lcc'),
            "cs_rcc": pre_ct_data.get('cs_rcc'),
            "cs_ncc": pre_ct_data.get('cs_ncc'),
            "hu": pre_ct_data.get('hu'),
            "implant_angle": pre_ct_data.get('implant_angle')
        })

        # PreEcho 추출
        pre_echo_data = entry.get('PreEcho', {})
        flat_entry.update({
            "pre_valve_velocity_peak": pre_echo_data.get('valve_velocity_peak'),
            "pre_valve_pressure_different_mean": pre_echo_data.get('valve_pressure_differentMean'),
            "lvedd": pre_echo_data.get('lvedd'),
            "lvef": pre_echo_data.get('lvef')
        })

        # PostEcho 추출
        post_echo_data = entry.get('PostEcho', {})
        flat_entry.update({
            "pl": post_echo_data.get('pl'),
            "post_valve_velovity_peak": post_echo_data.get('valve_velocity_peak'),
            "post_valve_pressure_different_mean": post_echo_data.get('valve_pressure_different_mean')
        })

        # BaselineEkg 추출
        baseline_ekg_data = entry.get('BaselineEkg', {})
        flat_entry.update({
            "rbbb": baseline_ekg_data.get('rbbb'),
            "lbbb": baseline_ekg_data.get('lbbb'),
            "pr_interval": baseline_ekg_data.get('pr_interval'),
            "first_av_block": baseline_ekg_data.get('first_av_block'),
            "qrs_interval": baseline_ekg_data.get('qrs_interval'),
            "qrs_abnormal": baseline_ekg_data.get('qrs_abnormal')
        })

        # PostEkg 추출
        post_ekg_data = entry.get('PostEkg', {})
        flat_entry.update({
            "dpr": post_ekg_data.get('dpr'),
            "dqrs": post_ekg_data.get('dqrs'),
            "new_rbbb": post_ekg_data.get('new_rbbb'),
            "new_lbbb": post_ekg_data.get('new_lbbb')
        })

        # Medicine 추출
        medicine_data = entry.get('Medicine', {})
        flat_entry.update({
            "asa": medicine_data.get('asa'),
            "except_asa": medicine_data.get('except_asa'),
            "anticoagulants": medicine_data.get('anticoagulants'),
            "bb": medicine_data.get('bb'),
            "ccb": medicine_data.get('ccb'),
            "acei_arb": medicine_data.get('acei_arb'),
            "diuretics": medicine_data.get('diuretics'),
            "aa": medicine_data.get('aa')
        })

        transformed_data.append(flat_entry)

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

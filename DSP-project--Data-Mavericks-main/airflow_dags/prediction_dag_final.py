import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException

PREDICTION_URL = 'http://backend:8000/predict'

FOLDER_A = "/usr/local/airflow/Folder_A"
FOLDER_B = "/usr/local/airflow/Folder_B"
FOLDER_C = "/usr/local/airflow/Folder_C"
LOGS = '/usr/local/airflow/Logs/log.txt'

FEATURE_SET = ['training_hours', 'city_development_index', 'gender']
CATEGORY_COLS = ['gender']
NUMERIC_COLS = ['training_hours', 'city_development_index']
TARGET = ['target']
GENDER_OPTIONS = ['Male', 'Female', 'Other', 'Male', 'Female', 'Other', 'Male', 'Female', 'nan', 'Female']

def verify_new_data(**kwargs):
    new_data = read_log(LOGS)

    if new_data:
        kwargs['task_instance'].xcom_push('new_data', new_data)
    else:
        raise AirflowSkipException

def predict(**kwargs):
    new_data = kwargs['task_instance'].xcom_pull(task_ids='verify_new_data', key='new_data')
    processed_data = prepare_data(new_data)
    result = requests.post(PREDICTION_URL, json=processed_data)

def read_log(filepath):
    with open(filepath, 'r') as file:
        lines = file.readlines()
    cleaned_lines = [line.strip() for line in lines]

    with open(filepath, 'w') as file:
        file.write('')
    
    return cleaned_lines
    
def prepare_data(filelist):
    df_list = []
    for filename in filelist:
        df = pd.read_csv(f'{FOLDER_C}/{filename}')
        df['gender'] = GENDER_OPTIONS
        df_list.append(df)
    
    df_combined = pd.concat(df_list)

    df_combined[CATEGORY_COLS] = df_combined[CATEGORY_COLS].fillna("nan")
    df_combined[NUMERIC_COLS] = df_combined[NUMERIC_COLS].fillna(0)
    df_combined[FEATURE_SET] = df_combined[FEATURE_SET].fillna(0)
    df_combined = df_combined.replace([np.inf, -np.inf], np.nan)

    processed_dict = df_combined.to_dict(orient='records')
    for item in processed_dict:
        for key, value in item.items():
            if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                item[key] = str(value)
    
    return processed_dict

predict_args = {
    "owner": "pardis",
    'depends_on_past':False,
    "start_date": datetime(2023, 6, 21),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

predict_dag = DAG(
    "prediction_job",
    default_args=predict_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
)

check_task = PythonOperator(
    task_id="verify_new_data",
    python_callable=verify_new_data,
    provide_context=True,
    dag=predict_dag,
)

predict_task = PythonOperator(
    task_id="execute_prediction",
    python_callable=predict,
    provide_context=True,
    dag=predict_dag,
)

check_task >> predict_task

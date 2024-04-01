import os
import random
import shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

FOLDER_A = "/usr/local/airflow/Folder_A"
FOLDER_B = "/usr/local/airflow/Folder_B"
FOLDER_C = "/usr/local/airflow/Folder_C"
LOGS = '/usr/local/airflow/Logs/log.txt'
#here instead of using great expectations randomly pick good or bad files
def ingest_files(**kwargs):
    input_files = os.listdir(FOLDER_A)
    picked_file = input_files[0]
    is_good = bool(random.choice([True, False]))

    if is_good:
        destination = FOLDER_C
        log_activity(picked_file)
    else:
        destination = FOLDER_B

    shutil.move(os.path.join(FOLDER_A, picked_file), os.path.join(destination, picked_file))

def log_activity(file):
    with open(LOGS, "a") as log:
        log.write(f"{file}\n")

ingest_args = {
    "owner": "pardis",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 21),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

ingest_dag = DAG(
    "data_ingestion",
    default_args=ingest_args,
    schedule_interval="*/2 * * * *", 
    catchup=False,
)

ingest_task = PythonOperator(
    task_id="check_and_ingest",
    python_callable=ingest_files,
    provide_context=True,
    dag=ingest_dag,
)

import os
import great_expectations as ge
from great_expectations.data_context import DataContext
import pytz
import shutil
import json
import requests
import psycopg2
from psycopg2 import Error
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# DAG configuration
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 6, 13),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "data_ingestion",
    default_args=default_args,
    schedule_interval="*/2 * * * *",  # Run every 2 minutes
    catchup=False,
)


def split_file(filename, row_limit=10, output_path='folder_A/', output_name_template='file_%s.csv'):
    """
    Splits a CSV file into multiple smaller ones based on a row limit.
    """
    data = pd.read_csv(filename)

    # Calculate the number of files needed
    num_files = len(data) // row_limit + (1 if len(data) % row_limit else 0)

    for i in range(num_files):
        # Slice data for each smaller file
        smaller_data = data[i * row_limit:(i + 1) * row_limit]
        smaller_data.to_csv(output_path + output_name_template % i, index=False)


split_file('aug_test.csv')

FOLDER_A = "/data/folder_A"
FOLDER_B = "/data/folder_B"
FOLDER_C = "/data/folder_C"

try:
    connection = psycopg2.connect(
        user="postgres",
        password="1234",
        host="localhost",
        port="5432",
        database="Hr_job_Change"
    )
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL:", error)


def save_statistics_to_db(connection, exception_results, filename):
    for result_key in exception_results.keys():
        exception_result = exception_results[result_key]["validation_result"]
        if not exception_result["success"]:
            for expectation_result in exception_result["results"]:
                if not expectation_result["success"]:
                    statistic = {
                        "filename": filename,
                        "expectation": expectation_result["expectation_config"]["expectation_type"],
                        "kwargs": expectation_result["expectation_config"]["kwargs"],
                        "result": expectation_result["result"],
                    }
                    insert_query = """
                    INSERT INTO statistics (filename, expectation, kwargs, result)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor = connection.cursor()
                    cursor.execute(insert_query, (
                    statistic["filename"], statistic["expectation"], json.dumps(statistic["kwargs"]),
                    json.dumps(statistic["result"])))
                    connection.commit()


context = DataContext.create(project_root_dir=FOLDER_A)

expectation_suite_name = "Hr_suite"
if expectation_suite_name not in context.list_expectation_suite_names():
    context.create_expectation_suite(expectation_suite_name)

datasource_name = "folder_A"
asset_name = "aug_train_split"
batching_regex = r".*\.csv"

for element in context.list_datasources():
    if datasource_name != element["name"]:
        datasource = context.sources.add_pandas_filesystem(name=datasource_name, base_directory=FOLDER_A)
        datasource.add_csv_asset(name=asset_name, batching_regex=batching_regex)

data_asset = context.get_datasource(datasource_name).get_asset(asset_name)
batch_request = data_asset.build_batch_request()

validator = context.get_validator(batch_request=batch_request, expectation_suite_name="Hr_suite")
existing_expectations = validator.get_expectation_suite().expectations

if not existing_expectations:
    validator.expect_table_column_count_to_equal(13)

    columns_list = ["enrollee_id", "city", "city_development_index", "gender", "relevent_experience",
                    "enrolled_university", "education_level", "major_discipline",
                    "experiences", "company_size", "company_type", "last_new_job", "training_hours"
                    ]
    for column_name in columns_list:
        validator.expect_column_to_exist(column=column_name)

    validator.expect_column_values_to_not_be_null(column="relevent_experience")
    validator.expect_column_values_to_not_be_null(column="education_level")
    validator.expect_column_values_to_not_be_null(column="major_discipline")
    validator.expect_column_values_to_not_be_null(column="training_hours")
    validator.expect_column_values_to_not_be_null(column="city")
    validator.expect_column_values_to_not_be_null(column="experiences")
    validator.expect_column_values_to_not_be_null(column="major_discipline")
    validator.expect_column_values_to_not_be_null(column="training_hours")

    validator.expect_column_values_to_be_in_set('gender', ['Male', 'Female', 'Other'])
    validator.expect_column_values_to_be_between('experience', '0', '>20')
    validator.expect_column_values_to_be_in_set('education_level',
                                                value_set=['Primary School', 'Graduate', 'Masters', 'High School',
                                                           'Phd'])

    validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint = ge.checkpoint.SimpleCheckpoint(
    name="Hr_checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "Hr_suite",
        },

    ],
)

checkpoint = context.get_checkpoint("Hr_checkpoint")
checkpoint_result = checkpoint.run()
context.build_data_docs()


def send_teams_alert(webhook_url, message):
    payload = {
        "text": message
    }

    response = requests.post(webhook_url, json.dumps(payload))

    if response.status_code != 200:
        print(f"Error sending Microsoft Teams alert. Status code: {response.status_code}")
    else:
        print("Microsoft Teams alert sent successfully.")


print(checkpoint_result)

overall_result = checkpoint_result.success
checkpoint_time = checkpoint_result.run_id.run_time
exception_results = checkpoint_result.run_results
exception_results_key = list(exception_results.keys())[0]
file_name = \
exception_results[exception_results_key]["validation_result"]["meta"]["active_batch_definition"]["batch_identifiers"][
    "path"]

if overall_result == True:
    shutil.move(FOLDER_A + "/" + file_name, FOLDER_C)
    print("The file was moved to good_quality_data folder")
else:
    shutil.move(FOLDER_A + "/" + file_name, FOLDER_B)
    print("The file was moved to bad_quality_data folder")
    save_statistics_to_db(connection, exception_results, file_name)

    webhook_url = "https://epitafr.webhook.office.com/webhookb2/6d8730c7-f7c0-42af-b8b2-361ef49bed8e@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/81d6ecd8246140c29b0c37413eb21de0/5c0ce808-b1ee-44f6-af80-e8ecdd92cd42"
    message = "Hi" + "There is a validation issue with the last uploaded file  "
    send_teams_alert(webhook_url, message)

data_ingestion_task = PythonOperator(
    task_id="data_ingestion",
    provide_context=True,
    python_callable=split_file,
    op_kwargs={"filename": "aug_train.csv", "row_limit": 10, "output_path": FOLDER_A,
               "output_name_template": "file_%s.csv"},
    dag=dag,
)

check_data_quality_task = PythonOperator(
    task_id="check_data_quality",
    provide_context=True,
    python_callable=raise_error,
    dag=dag,
)

data_ingestion_task >> check_data_quality_task

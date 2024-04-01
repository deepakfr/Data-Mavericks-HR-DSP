# ML powered app
## by team Data Mavericks



Develop a machine learning (ML) architecture for HR to predict job churn. There are six major features including the ML model and the app code. The goals is to orchestrate various components to operationalize the app, as well as handle data injection, predictions, quality, and user interactions (interface).


- Learn new components
- Develop features
- ✨Magic ✨

## Dataset

Dataset: 
https://www.kaggle.com/datasets/arashnic/hr-analytics-job-change-of-data-scientists 

## Features

- Data injestion and prediction with Airflow
- Data quality with Great Expectations
- Store results in Db PostgreSQL
- Visualize data inconsistencies with Grafana
- Exposing the ML mode and saving predictions with FastAPI
- A user interface where the user can make on-demand predictions and view past predictions (streamlit)


## Tech

ML application uses a number of open source projects to work properly:

- [AirFlow] - A prediction job to make scheduled prediction
- [GE] - An ingestion job to ingest and validate the data quality
- [FastAPI] - Saving the predictions to the database
- [PostgreSQL] - An SQL database for saving data
- [Streamlight] - A user interface where the user can make on-demand predictions and view past predictions
- [Grafana] - A monitoring dashboard to monitor data quality problems during injestion


## Installation


Example installation of the AirFlow.

```sh
cd airflow
export AIRFLOW_HOME=~/airflow
```

Install Airflow using the constraints file, which is determined based on the URL we pass:...

```sh
AIRFLOW_VERSION=2.6.1

# Extract the version of Python you have installed. If you're currently using Python 3.11 you may want to set this manually as noted above, Python 3.11 is not yet supported.
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.6.1 with python 3.7: https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.7.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

## Plugins

We are currently extended with the following plugins.
Instructions on how to use them in your own application are linked below.

| Plugin | README |
| ------ | ------ |
| Dropbox | [plugins/dropbox/README.md][PlDb] |
| GitHub | [plugins/github/README.md][PlGh] |
| Google Drive | [plugins/googledrive/README.md][PlGd] |
| OneDrive | [plugins/onedrive/README.md][PlOd] |
| Medium | [plugins/medium/README.md][PlMe] |
| Google Analytics | [plugins/googleanalytics/README.md][PlGa] |

## Development


We used PyCharm for fast developing.

Below is an example for installation of Great Expectation on Mac. Open your favorite Terminal and run these commands.

Install in terminal:

```sh
python -m pip install great_expectations
```


Verify:

```sh
great_expectations --version
```

This should return something like::

```sh
great_expectations, version 0.16.16
```

#### Setting up the database

For PostgreSQL:

```sh
brew install postgresql@14
```

Run the command:

```sh
ln -sfv /usr/local/opt/postgresql/*.plist ~/Library/LaunchAgents
```

## DAGs

A workflow is represented as a DAG (a Directed Acyclic Graph), and contains individual pieces of work called Tasks (validate data quality for example).

DAG (Directed Acyclic Graph): collection of tasks organized in a way that reflects their relationships and dependencies

DAG run:

Physical instance of a DAG, containing task instances that run for a specific execution_date.
They are created by the Airflow scheduler or another trigger (manual trigger for example)

```sh
dag = DAG(
    "data_ingestion",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # Run every 1 minute
    catchup=False,
    tasks=[data_ingestion_task, notify_task, teams_notification]
)
```

## License

MIT

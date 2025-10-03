# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
import logging
import os
from pendulum import duration
import json

# -------------------- #
# Enter your own info! #
# -------------------- #

MY_NAME = "Mathilde M."
PROJECT = "francetravail_elt"
# ----------------------- #
# Configuration variables #
# ----------------------- #

# MinIO connection config
MINIO_ACCESS_KEY = json.loads(os.environ["AIRFLOW_CONN_MINIO_DEFAULT"])["login"]
MINIO_SECRET_KEY = json.loads(os.environ["AIRFLOW_CONN_MINIO_DEFAULT"])["password"]
MINIO_IP = json.loads(os.environ["AIRFLOW_CONN_MINIO_DEFAULT"])["host"]
JOB_MARKET_BUCKET_NAME = "job-market"
ROME_JOB_BUCKET_NAME = "rome-job"
CODES_OBJECT_NAME = "list_rome_job.json"
ARCHIVE_BUCKET_NAME = "archive"

# Source file path climate data #TODO no required
TEMP_GLOBAL_PATH = f"{os.environ['AIRFLOW_HOME']}/include/climate_data/temp_global.csv"

# Datasets
DS_JOB_MARKET_DATA_MINIO = Dataset(f"{PROJECT}://minio:/{JOB_MARKET_BUCKET_NAME}")
DS_ROME_JOB_DATA_MINIO = Dataset(f"{PROJECT}://minio:/{ROME_JOB_BUCKET_NAME}")
DS_DUCKDB_JOB_MARKET = Dataset(f"{PROJECT}://duckdb/mart")
DS_DUCKDB_REPORTING = Dataset(f"{PROJECT}://duckdb/reporting")
DS_START = Dataset("start") #TODO à quoi ça sert

# DuckDB config
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
JOB_MARKET_IN_TABLE_NAME = "in_job_market"
REPORTING_TABLE_NAME = "reporting_table"
CONN_ID_DUCKDB = "duckdb_default"

# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# DAG default arguments
default_args = {
    "owner": MY_NAME,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=5),
}


# command to run streamlit app within codespaces/docker #TODO
# modifications are necessary to support double-port-forwarding
#STREAMLIT_COMMAND = "streamlit run weather_v_climate_app.py --server.enableWebsocketCompression=false --server.enableCORS=false"
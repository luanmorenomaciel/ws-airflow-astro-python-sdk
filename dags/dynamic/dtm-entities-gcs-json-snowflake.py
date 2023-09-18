"""
Using the task group alongside with the dynamic task mapping capability
to dynamically list and ingest files from gcs into snowflake.

Astro Python SDK leverages the DTM feature using
the expand and partial command to map and process files at scale.
"""

# import libraries
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata
from astro.files import get_file_list
from astro.sql.operators.load_file import LoadFileOperator as LoadFile

# connections
GCS_CONN_ID = "google_cloud_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


# declare dag
@dag(
    dag_id="dtm-entities-gcs-json-snowflake",
    start_date=datetime(2023, 4, 12),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'loading', 'gcs', 'snowflake']
)
# init main function
def load_data():

    # init and end task
    split_files_by_metadata = DummyOperator(task_id="split_files_by_metadata")
    finish_loading_to_dw_process = DummyOperator(task_id="finish_loading_to_dw_process")

    # metadata layer
    # backend store {cosmosdb}
    sources = [
        {'entity': 'users', 'url': 'gs://owshq-landing-zone/users/'},
        {'entity': 'transactions', 'url': 'gs://owshq-landing-zone/transactions/'},
        {'entity': 'stocks', 'url': 'gs://owshq-landing-zone/stocks/'}
    ]

    # loop through files
    # read metadata function
    for source in sources:

        # task group = set of tasks
        # based on how many lines read
        with TaskGroup(group_id=source["entity"]) as task_group_entities:

            # dynamic task mapping
            # load from storage into dw
            load_from_storage_dw = LoadFile.partial(
                task_id="load_from_storage_dw",
                output_table=Table(name=source["entity"], conn_id=SNOWFLAKE_CONN_ID),
                if_exists="replace",
                use_native_support=True,
                columns_names_capitalization="original"
            ).expand(input_file=get_file_list(path=source["url"], conn_id=GCS_CONN_ID))

            # trigger action
            load_from_storage_dw

        # define sequence
        split_files_by_metadata >> task_group_entities >> finish_loading_to_dw_process


# init dag
dag = load_data()
"""
Use the astro python sdk library to load data from
the google cloud storage {gcs} and write into snowflake.

Doesn't apply any transformation during loading time.
"""

# import libraries
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from airflow.operators.dummy import DummyOperator

import pandas as pd
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
LANDING_ZONE_PATH = "gs://owshq-landing-zone/"
SOURCE_CONN_ID = "google_cloud_default"
OUTPUT_CONN_ID = "snowflake_default"

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


@dag(
    dag_id="gcs-users-json-snowflake",
    start_date=datetime(2023, 4, 12),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 'gcs', 'snowflake']
)
# init main function
def load_data():

    # init task
    init_data_load = DummyOperator(task_id="init")

    # load data
    user_data = aql.load_file(
        input_file=File(path=LANDING_ZONE_PATH + "users", filetype=FileType.JSON, conn_id=SOURCE_CONN_ID),
        output_table=Table(name="users", conn_id=OUTPUT_CONN_ID),
        task_id="user_data",
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # finish task
    finish_data_load = DummyOperator(task_id="finish")

    # define sequence
    init_data_load >> user_data >> finish_data_load


# init dag
dag = load_data()
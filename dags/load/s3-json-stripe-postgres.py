"""
Use the astro python sdk library to load data from
minio {s3} into the postgres database.

Doesn't apply any transformation during loading time.
"""

# import libraries
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

import pandas as pd
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
BUCKET_PATH = "landing"
S3_CONN_ID = "aws_default"
POSTGRES_CONN_ID = "postgres_conn"

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


# declare dag
@dag(
    dag_id="s3-json-stripe-postgres",
    start_date=datetime(2023, 4, 13),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 's3', 'postgres']
)
# init main function
def elt_with_transforms():

    # init task
    init_data_load = EmptyOperator(task_id="init")

    # users group
    with TaskGroup(group_id="Stripe") as stripe_task_group:

        load_s3_to_database = aql.load_file(
            task_id="load_s3_to_database",
            input_file=File(path="s3://landing/stripe/stripe_2023_4_13", filetype=FileType.JSON, conn_id=S3_CONN_ID),
            output_table=Table(name="stripe", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="astro")),
            if_exists="replace",
            use_native_support=True,
            columns_names_capitalization="original"
        )

    # task {sequence}
    load_s3_to_database

    # finish task
    finish_data_load = EmptyOperator(task_id="finish")

    # define sequence
    init_data_load >> stripe_task_group >> finish_data_load


# init dag
dag = elt_with_transforms()
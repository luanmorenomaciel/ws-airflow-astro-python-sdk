"""
Using the load file operator to load data from s3 into pandas dataframe in-memory.

Passing the dataframe into the {dataframe} operator that will execute
a pandas dataframe operation.
"""

# import libraries
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

import pandas as pd
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
S3_CONN_ID = "aws_default"
OUTPUT_CONN_ID = "snowflake_default"

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


# dataframe {}
@aql.dataframe(columns_names_capitalization="original")
def most_active_plans(subscription: pd.DataFrame):
    print(f"total number of records: {len(subscription)}")
    grouped_plans = subscription.groupby(['plan'])['plan'].count()
    print(f"platform plans: {grouped_plans}")
    return grouped_plans


# declare dag
@dag(
    dag_id="dataframe-user-subscription",
    start_date=datetime(2023, 4, 13),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 's3', 'dataframe']
)
# init main function
def dataframe_etl():

    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    # load files {subscription}
    subscription_file = aql.load_file(
        task_id="subscription_file",
        input_file=File(path="s3://landing/subscription/subscription_2023_4_13_18", filetype=FileType.JSON, conn_id=S3_CONN_ID),
    )

    # invoke dataframe function {pandas}
    plans = most_active_plans(subscription=subscription_file)

    # define sequence
    init_data_load >> subscription_file >> plans >> finish_data_load


# init dag
dag = dataframe_etl()
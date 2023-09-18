"""
The load file operator from Astro Python SDK allows
to load data into a dataframe in-memory that could be used to
apply transformations before loading into the data warehouse.

This pipeline shows how to store into a dataframe without
specifying an output table.
"""

# import libraries
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from astro import sql as aql
from astro.files import File
from astro.constants import FileType

# connections
BUCKET_PATH = "landing"
S3_CONN_ID = "aws_default"

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


# declare dag
@dag(
    dag_id="s3-user-subscription-pandas-df",
    start_date=datetime(2023, 4, 13),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 's3', 'pandas', 'dataframe']
)
# init main function
def pandas_dataframe():

    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    df_user = aql.load_file(
        task_id="df_user",
        input_file=File(path="s3://landing/user/user_2023_4_13", filetype=FileType.JSON, conn_id=S3_CONN_ID),
    )

    df_subscription = aql.load_file(
        task_id="df_subscription",
        input_file=File(path="s3://landing/subscription/subscription_2023_4_13", filetype=FileType.JSON, conn_id=S3_CONN_ID),
    )

    # define sequence
    init_data_load >> [df_user, df_subscription] >> finish_data_load


# init dag
dag = pandas_dataframe()
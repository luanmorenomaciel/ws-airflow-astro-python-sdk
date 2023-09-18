"""
Using the load file operator to ingest data from s3 into Snowflake as persistent tables.

Using the {transform} operator to join these two tables from the Snowflake database
and write into a new output table.

The transform operator use SQL language abstraction to perform any type
of sql processing.
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


# transforms {}
@aql.transform
def join_tables(user: Table, subscription: Table):
    return """
    SELECT u.user_id,
           u.first_name,
           u.last_name,
           u.email,
           u.gender,
           u.date_of_birth,
           u.employment,
           u.credit_card,
           u.subscription,
           s.plan,
           s.status,
           s.payment_method,
           s.subscription_term
    FROM {{ user }} AS u 
    INNER JOIN {{ subscription }} AS s
    ON u.user_id = s.user_id
    """


# declare dag
@dag(
    dag_id="transform-user-subscription",
    start_date=datetime(2023, 4, 13),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 's3', 'transform']
)
# init main function
def transform_etl():

    # init & finish task
    init_data_load = EmptyOperator(task_id="init")
    finish_data_load = EmptyOperator(task_id="finish")

    # users
    df_user = aql.load_file(
        task_id="df_user",
        input_file=File(path="s3://landing/user/user_2023_4_13", filetype=FileType.JSON, conn_id=S3_CONN_ID),
        output_table=Table(name="user", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro"),),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # subscriptions
    df_subscription = aql.load_file(
        task_id="df_subscription",
        input_file=File(path="s3://landing/subscription/subscription_2023_4_13", filetype=FileType.JSON, conn_id=S3_CONN_ID),
        output_table=Table(name="subscription", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro"),),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # join datasets using query logic
    # storing output in a table
    join_datasets = join_tables(
        user=df_user,
        subscription=df_subscription,
        output_table=Table(name="subscriptions", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro")),
    )

    # define sequence
    init_data_load >> [df_user, df_subscription] >> join_datasets >> finish_data_load


# init dag
dag = transform_etl()
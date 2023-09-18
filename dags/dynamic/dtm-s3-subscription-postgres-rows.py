"""
Dynamic Task Mapping used with Astro Python SDK that does the following:

- load data from s3 into postgres database using load file operator
- execute a raw sql {run_raw_sql} with fetch all results to airflow's executor node
- expands the result set based on the amount of data collected on the previous task

"""

# import libraries
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task

import pandas as pd
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
S3_CONN_ID = "aws_default"
POSTGRES_CONN_ID = "postgres_conn"

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


# taskflow api {task}
@task
def summarize_plans(plans: str):
    print(plans)


# [START howto_run_raw_sql_with_handle]
def handle_result(result):
    return result.fetchall()


# declare dag
@dag(
    dag_id="dtm-s3-subscription-postgres-rows",
    start_date=datetime(2023, 4, 13),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 's3', 'postgres']
)
# init main function
def dtm_result_set():

    # retrieve data from database
    @aql.run_raw_sql(handler=handle_result)
    def get_plans(table: Table):
        return """select distinct plan from {{ table }}"""

    # load data
    load_subscription_postgres = aql.load_file(
        task_id="load_subscription_postgres",
        input_file=File(path="s3://landing/subscription/subscription_2023_4_13", filetype=FileType.JSON, conn_id=S3_CONN_ID),
        output_table=Table(name="subscription", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="astro")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    plan_types = get_plans(load_subscription_postgres)
    # [END howto_run_raw_sql_with_handle]

    summarize_plans.expand(plans=plan_types)


# init dag
dag = dtm_result_set()
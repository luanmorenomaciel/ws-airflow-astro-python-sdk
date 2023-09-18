"""
Performing validation at the table level using {checks}.

https://docs.astronomer.io/learn/airflow-sql-data-quality
"""

# import libraries
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

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


# declare dag
@dag(
    dag_id="check-table-stripe",
    start_date=datetime(2023, 4, 13),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 's3', 'postgres']
)
# init main function
def check_table_data():

    # init task
    init_data_load = EmptyOperator(task_id="init")

    # load file
    load_s3_to_database = aql.load_file(
        task_id="load_s3_to_database",
        input_file=File(path="s3://landing/stripe/stripe_2023_4_13_18", filetype=FileType.JSON, conn_id=S3_CONN_ID),
        output_table=Table(name="stripe", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="astro")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # [START data_validation__check_table]
    check_table_stripe = aql.check_table(
        task_id="check_table_stripe",
        dataset=load_s3_to_database,
        checks={
            "row_count": {"check_statement": "Count(*) > 100"},
            "year": {"check_statement": "year IS NOT NULL"},
            "month": {"check_statement": "month IS NOT NULL"},
            "token": {"check_statement": "token "}
        },
    )
    # [END data_validation__check_table]

    # finish task
    finish_data_load = EmptyOperator(task_id="finish")

    # task {sequence}
    init_data_load >> load_s3_to_database >> check_table_stripe >> finish_data_load


# init dag
dag = check_table_data()
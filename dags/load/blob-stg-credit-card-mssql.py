"""
Use the astro python sdk library to load data from
blob storage and write into sql server engine (Azure SQL Database).

Doesn't apply any transformation during loading time.
"""

# import libraries
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
SOURCE_CONN_ID = "wasb_conn"
OUTPUT_CONN_ID = "mssql_conn"

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


@dag(
    dag_id="blob-stg-credit-card-mssql",
    start_date=datetime(2023, 4, 13),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 'blob storage', 'mssql']
)
# init main function
def load_data():

    # init task
    init_data_load = DummyOperator(task_id="init")

    # load data
    credit_card_data = aql.load_file(
        input_file=File("wasb://landing/credit_card/credit_card_2023_1_25_", filetype=FileType.JSON, conn_id=SOURCE_CONN_ID),
        output_table=Table(name="credit_card", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro"),),
        task_id="credit_card_data",
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # finish task
    finish_data_load = DummyOperator(task_id="finish")

    # define sequence
    init_data_load >> credit_card_data >> finish_data_load


# init dag
dag = load_data()
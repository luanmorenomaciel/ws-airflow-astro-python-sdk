"""
Loading data from s3 into postgres database and
saving the output also on the data lake (s3) in
apache parquet format.

Could be used to load data from landing zone
and write in a optimized format for data engineering pipelines.

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
    dag_id="s3-vehicle-postgres-export-parquet",
    start_date=datetime(2023, 4, 13),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk', 's3', 'postgres', 'parquet']
)
# init main function
def export_file():

    # load data
    load_vehicle_data = aql.load_file(
        task_id="load_vehicle_data",
        input_file=File(path="s3://landing/vehicle/vehicle_2023_4_13", filetype=FileType.JSON, conn_id=S3_CONN_ID),
        output_table=Table(name="vehicle", conn_id=POSTGRES_CONN_ID, metadata=Metadata(schema="astro")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # export file
    aql.export_to_file(
        task_id="export_to_parquet",
        input_data=load_vehicle_data,
        output_file=File(path="s3://processing/vehicle/vehicle.parquet"),
        if_exists="replace",
    )


# init dag
dag = export_file()
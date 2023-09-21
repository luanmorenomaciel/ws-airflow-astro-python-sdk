"""
"""

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

import pandas as pd

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
S3_CONN_ID = "aws_default"
CURATED_ZONE = "curated"
OUTPUT_CONN_ID = "postgres_conn"

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


subscriptions_dataset = "astro+s3://aws_default@curated/subscriptions.parquet"


@dag(
    dag_id="dataset-consumer",
    start_date=datetime(2023, 4, 14),
    max_active_runs=1,
    schedule=[Dataset(subscriptions_dataset)],
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk']
)
def etl_consumer():

    subscriptions = aql.load_file(
        task_id="load_subscriptions",
        input_file=File(path=subscriptions_dataset, conn_id=S3_CONN_ID),
        use_native_support=True,
        columns_names_capitalization="original"
    )
    subscriptions

dag = etl_consumer()

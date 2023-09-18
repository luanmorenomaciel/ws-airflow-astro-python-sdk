# TODO always develop your DAGs using TaskFlowAPI
"""
Tasks performed by this DAG:
"""

# import libraries
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections & variables
SOURCE_CONN_ID = "google_cloud_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
BIGQUERY_CONN_ID = "google_cloud_default"

# default args
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


# declare dag
@dag(
    dag_id="demo-gcs-users-json-warehouses",
    start_date=datetime(2023, 4, 15),
    schedule_interval=timedelta(hours=24),
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
    owner_links={"linkedin": "https://www.linkedin.com/in/luanmoreno/"},
    tags=['gcs', 'json', 'users', 'astrosdk', 'snowflake', 'bigquery']
)
# declare main function
def load_files_warehouse():

    # init & finish
    init = EmptyOperator(task_id="init")
    finish = EmptyOperator(task_id="finish")

    # ingest from lake to snowflake
    users_json_files_snowflake = aql.load_file(
        task_id="users_json_files_snowflake",
        input_file=File(path="gs://owshq-landing-zone/users", filetype=FileType.JSON, conn_id=SOURCE_CONN_ID),
        output_table=Table(name="users", conn_id=SNOWFLAKE_CONN_ID),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # ingest from lake to bigquery
    users_json_files_bigquery = aql.load_file(
        task_id="users_json_files_bigquery",
        input_file=File(path="gs://owshq-landing-zone/users", filetype=FileType.JSON, conn_id=SOURCE_CONN_ID),
        output_table=Table(name="users", metadata=Metadata(schema="OwsHQ"), conn_id=BIGQUERY_CONN_ID),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # define sequence
    init >> [users_json_files_snowflake, users_json_files_bigquery] >> finish


# init
dag = load_files_warehouse()

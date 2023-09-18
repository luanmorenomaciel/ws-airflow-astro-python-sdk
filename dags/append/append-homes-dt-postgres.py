# import libraries
import pathlib
from datetime import datetime

from airflow.models import DAG

from astro import sql as aql
from astro.files import File
from astro.table import Table

# file loc
CWD = pathlib.Path(__file__).parent

# default args
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}

# init dag
dag = DAG(
    "append-homes-dt-postgres",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 4, 14),
    catchup=False,
    tags=['development', 'astrosdk', 'pandas', 'postgres']
)

# loc files
DATA_DIR = str(CWD) + "/data/"

# declare dag
with dag:

    # load main file
    target_table = aql.load_file(
        task_id="target_table",
        input_file=File(path=DATA_DIR + "homes_2023.csv"),
        output_table=Table(conn_id="postgres_conn"),
    )

    # load append file
    source_table = aql.load_file(
        task_id="source_table",
        input_file=File(path=DATA_DIR + "homes_2022.csv"),
        output_table=Table(conn_id="postgres_conn"),
    )
    
    # [START append_example]
    append_data = aql.append(
        task_id="append_data",
        target_table=target_table,
        source_table=source_table,
    )
    # [END append_example]

    # [START append_example_col_dict]
    aql.append(target_table=target_table, source_table=source_table, columns={"beds": "baths"})
    # [END append_example_col_dict]
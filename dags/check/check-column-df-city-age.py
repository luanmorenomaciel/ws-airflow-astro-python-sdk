"""
Apply column level validation using the native Airflow way.

# https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/operators.html#check-sql-table-columns
"""

# import libraries
from datetime import datetime

import pandas as pd
from airflow import DAG

from astro import sql as aql

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}

# init dag
with DAG(
    "check-column-df-city-age",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 4, 13),
    catchup=False,
    tags=['development', 'astrosdk', 'pandas', 'dataframe']
) as dag:

    # [START data_validation__check_column]
    df = pd.DataFrame(
        data={
            "name": ["Luan Moreno", "Mateus Oliveira"],
            "age": [34, None],
            "city": ["DF", "BH"],
            "user_id": [1, 2],
        }
    )

    aql.check_column(
        dataset=df,
        column_mapping={
            "name": {"null_check": {"geq_to": 0, "leq_to": 1}},
            "city": {
                "null_check": {
                    "equal_to": 1,
                },
            },
            "age": {
                "null_check": {
                    "equal_to": 1,
                    "tolerance": 1,
                },
            },
        },
    )
    # [END data_validation__check_column]
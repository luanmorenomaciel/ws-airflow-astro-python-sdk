"""
"""

# import libraries
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator

import pandas as pd
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# connections
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
CURATED_ZONE = "curated"
OUTPUT_CONN_ID = "postgres_conn"

# default args & init dag
default_args = {
    "owner": "luan moreno m. maciel",
    "retries": 1,
    "retry_delay": 0
}


# START_transforms {sql}
@aql.transform
def sanitize_user(user: Table):
    return """
        SELECT DISTINCT u.user_id,
        CONCAT(u.first_name, ' ', u.last_name) AS name,
        u.email,
        u.gender,
        u.phone_number,
        u.date_of_birth,
        CAST(REPLACE(u.employment, '''', '"') AS JSONB)->>'title' AS title,
        CAST(REPLACE(u.subscription, '''', '"') AS JSONB)->>'payment_method' AS method
    FROM {{ user }} AS u;
    """


@aql.transform
def sanitize_subscription(subscription: Table):
    return """
        SELECT s.id,
        s.user_id,
        s.plan,
        s.status,
        s.subscription_term AS type,
        s.payment_term AS term
        FROM {{ subscription }} AS s;
    """


@aql.transform
def join_tables(user: Table, subscription: Table):
    return """
    SELECT u.user_id,
           u.name,
           u.email,
           u.gender,
           u.phone_number,
           u.date_of_birth,
           u.title,
           u.method,
           s.plan,
           s.status,
           s.type,
           s.term,
           s.importance
    FROM {{ user }} AS u 
    INNER JOIN {{ subscription }} AS s
    ON u.user_id = s.user_id
    """
# END_transforms {sql}


# START_dataframes {pandas}
@aql.dataframe()
def plan_importance(subscription: pd.DataFrame):
    subscription["importance"] = subscription["plan"].apply(lambda types: "high" if types in('Business','Diamond','Gold','Platinum','Premium') else "low")
    return subscription
# END_dataframes {pandas}


# declare dag
@dag(
    dag_id="etl-subscriptions-analysis",
    start_date=datetime(2023, 4, 14),
    max_active_runs=1,
    schedule_interval=timedelta(hours=24),
    default_args=default_args,
    catchup=False,
    tags=['development', 'elt', 'astrosdk']
)
# init main function
def etl_subscriptions_analysis():

    # init & finish task
    init = EmptyOperator(task_id="init")
    finish = EmptyOperator(task_id="finish")

    # TODO = get daily files
    # user
    user_file = aql.load_file(
        task_id="user_file",
        input_file=File(path="s3://landing/user/user_2023_4_14_16", filetype=FileType.JSON, conn_id=S3_CONN_ID),
        output_table=Table(name="user", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # subscription
    subscription_file = aql.load_file(
        task_id="subscription_file",
        input_file=File(path="s3://landing/subscription/subscription_2023_4_14_16", filetype=FileType.JSON, conn_id=S3_CONN_ID),
        output_table=Table(name="subscription", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro")),
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # sanitize sql queries
    # execute on database {side}
    # TODO append or replace capability
    user_sanitize = sanitize_user(
        user=user_file,
        output_table=Table(name="sanitized_user", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro"))
    )

    subscription_sanitize = sanitize_subscription(
        subscription=subscription_file,
        output_table=Table(name="sanitized_subscription", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro"))
    )

    # apply function to enrich dataset
    # save data into database after transformation
    verify_plans_subscriptions = plan_importance(
        subscription=subscription_sanitize
    )

    # join tables for final delivery
    # new table on database {final}
    final_curated_subscriptions_ds = join_tables(
        user=user_sanitize,
        subscription=verify_plans_subscriptions,
        output_table=Table(name="subscriptions", conn_id=OUTPUT_CONN_ID, metadata=Metadata(schema="astro")),
    )

    # check final dataset {before}
    # quality rules
    quality_rules_check_table_level = aql.check_table(
        task_id="quality_rules_check_table_level",
        dataset=final_curated_subscriptions_ds,
        checks={
            "row_count": {"check_statement": "Count(*) > 0"},
            "method": {"check_statement": "method IS NOT NULL"},
            "type": {"check_statement": "method IS NOT NULL"},
            "importance": {"check_statement": "importance IN ('low','high')"}
        },
    )

    # export to s3 storage
    # curated zone in parquet format
    export_to_s3_curated_zone = aql.export_to_file(
        task_id="export_to_s3_curated_zone",
        input_data=final_curated_subscriptions_ds,
        output_file=File(path="s3://curated/{{ ds }}/subscriptions.parquet", conn_id=S3_CONN_ID),
        if_exists="replace",
    )

    # load into snowflake warehouse
    subscriptions_curated_snowflake = aql.load_file(
        input_file=File(path="s3://curated/{{ ds }}/subscriptions.parquet", filetype=FileType.PARQUET, conn_id=S3_CONN_ID),
        output_table=Table(name="subscriptions", conn_id=SNOWFLAKE_CONN_ID, metadata=Metadata(schema="astro")),
        task_id="subscriptions_curated_snowflake",
        if_exists="replace",
        use_native_support=True,
        columns_names_capitalization="original"
    )

    # define sequence
    chain(
        init,
        [user_file, subscription_file], [user_sanitize, subscription_sanitize],
        verify_plans_subscriptions,
        final_curated_subscriptions_ds,
        quality_rules_check_table_level,
        export_to_s3_curated_zone,
        subscriptions_curated_snowflake,
        finish
    )


# init dag
dag = etl_subscriptions_analysis()
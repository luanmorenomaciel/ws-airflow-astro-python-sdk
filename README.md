# Developing Modern ETL Pipelines on Apache Airflow with Astro Python SDK
https://airflowsummit.org/sessions/2023/workshops/astro-python-sdk-developing-modern-etl-pipelines-on-apache-airflow/

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Airflowctl](https://github.com/kaxil/airflowctl)
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- [Astro Python SDK](https://github.com/astronomer/astro-sdk)

### Description
This workshop has the intent to show how to use the Astro Python SDK to develop modern ETL pipelines on Apache Airflow. 
The Astro Python SDK is a library that allows you to write Airflow DAGs in Python, and it provides a set of operators that allow you to interact with data. 
The workshop will cover the following topics:

- Understanding the Astro Python SDK
- The Astro Python SDK Operators
- Developing DAGs with the Astro Python SDK
- Building and end-to-end data pipeline with the Astro Python SDK

### Install & Init astro-cli for dev env
Install and init astro-cli to develop locally
```shell
https://github.com/astronomer/astro-cli
brew install astro

astro dev init
requirement.txt
astro dev start

http://localhost:8080
```

### Astro Python SDK
Use Astro Python SDK to develop DAGs in an effortless way

https://docs.astronomer.io/astro/develop-project
https://docs.astronomer.io/learn/astro-python-sdk-etl
https://astro-sdk-python.readthedocs.io/en/stable/

```shell
pip install apache-airflow
pip install astro-sdk-python
```

Operators:

- append 
- cleanup 
- dataframe 
- drop table 
- export_to_file 
- get_value_list
- load_file 
- merge 
- run_raw_sql 
- transform 
- transform_file 
- check_column 
- check_table 
- get_file_list

### DAGs & Astro Python SDK Operators
Build DAGs using Astro Python SDK Operators. Configure connections on Airflow UI.

Connections:
- google_cloud_default
ws-astro-python-sdk@silver-charmer-243611.iam.gserviceaccount.com

- snowflake_default

- aws_default
endpoint_url: http://20.122.206.152
accessKey: data-lake
secretKey: 12620ee6-2162-11ee-be56-0242ac120002

Use-Cases:
- Load = allows loading from different object-storage systems to destinations.  
    - gcs-users-json-snowflake
    - s3-user-subscription-pandas-df
  
- DataFrame = allows you to run python transformations 
  - dataframe-user-subscription

- Transform = allows you to implement the t of an elt system by running a sql query.
  - transform-user-subscription

- Check = allows you to add checks on columns of tables and dataframes.
  - check-column-df-city-age 
  - check-table-stripe
  
- Export = 
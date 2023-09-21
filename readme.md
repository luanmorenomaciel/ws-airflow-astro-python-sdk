# Developing Modern ETL Pipelines on Apache Airflow with Astro Python SDK
https://airflowsummit.org/sessions/2023/workshops/astro-python-sdk-developing-modern-etl-pipelines-on-apache-airflow/

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
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

### 1) Install Docker Desktop
Install docker desktop to run airflow locally
```shell
https://www.docker.com/products/docker-desktop/
```

### 2) Install Astro-CLI
Install astro-cli to develop DAGs
```shell
https://github.com/astronomer/astro-cli

curl -sSL install.astronomer.io | sudo bash -s
brew install astro

astro dev init
```

### 3) Add Airflow Connections
Add these configurations into the airflow_settings.yaml file
```yaml
airflow:
  connections:
    - conn_id: aws_default
      conn_type: aws
      conn_schema:
      conn_login: data-lake
      conn_password: 12620ee6-2162-11ee-be56-0242ac120002
      conn_port:
      conn_extra:
        endpoint_url: http://20.122.206.152
    - conn_id: postgres_conn
      conn_type: postgres
      conn_host: postgres
      conn_schema: postgres
      conn_login: postgres
      conn_password: postgres
      conn_port: 5432
```

### 4) Init Airflow Project
Initialize project using the astro-cli
```shell
astro dev start
http://localhost:8080
```

NOTE - behind the scenes, astro is using docker to setup services. If you run into port bind issue (for example, `Error: error building, (re)creating or starting project containers: Error response from daemon: Ports are not available: exposing port TCP 127.0.0.1:5432 -> 0.0.0.0:0: listen tcp 127.0.0.1:5432: bind: address already in use`), you'll need to stop the process that's using the conflicting port.

What's going on is that your Mac already has Postgres running on that port. 

To resolve this when using `brew`, stop the Postgres process.
1. Find the exact name of the installed postgres instance, `brew services list`
2. Stop the postgres service, `brew services stop postgresql@14`

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
Build DAGs using Astro Python SDK Operators.

Connections:
- aws_default  
endpoint_url: http://20.122.206.152  
accessKey: data-lake  
secretKey: 12620ee6-2162-11ee-be56-0242ac120002  

Use-Cases:
- Load = allows loading from different object-storage systems to destinations.  
  - s3-json-stripe-postgres
  - s3-user-subscription-pandas-df
  
- DataFrame = allows you to run python transformations 
  - dataframe-user-subscription

- Transform = allows you to implement the t of an elt system by running a sql query.
  - transform-user-subscription

- Check = allows you to add checks on columns of tables and dataframes.
  - check-column-df-city-age 
  - check-table-stripe
  
- Export = allows you to write sql tables to csv or parquet files and store them locally, on s3, or on gcs.
  - s3-vehicle-postgres-export-parquet

### Building an End to End Data Pipeline using Astro Python SDK
```shell

```
#!/bin/bash

# Set up connections
echo ">>> Setting up Airflow connections"

airflow connections add 'HIVE_DW_CONNECTION' \
    --conn-json '{
        "conn_type": "beeline",
        "host": "jdbc:hive2://hive-server",
        "port": 10000,
        "schema": "default"
    }'

airflow connections add 'AIRFLOW_DB_CONNECTION' \
    --conn-json '{
        "conn_type": "postgres",
        "login": "airflow",
        "password": "airflow",
        "host": "postgres",
        "port": 5432,
        "schema": "airflow"
    }'

airflow connections add 'SPARK_CONNECTION' \
    --conn-json '{
        "conn_type": "spark",
        "host": "spark://spark-master",
        "port": 7077
    }'

# Set up variables
# echo ">> Setting up airflow variables"
# airflow variables set SQL_CHUNK_SIZE 20
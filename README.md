# DEBC_PRACTICES
A project with the final exercises using the knowledge acquired in the bootcamp. 

# Exercise_#01
The exercise is based on the edvai_hadoop container as the environment. 
The user should copy the different files into the Airflow folders:


docker cp scripts/ edvai_hadoop:/home/hadoop/airflow/dags/
docker cp flights_data_transformation_and_load.py edvai_hadoop:/home/hadoop/scripts/
docker cp ingest_flights_data_pipeline.py edvai_hadoop:/home/hadoop/airflow/dags/


Also, before to run the Airflow DAG we should to create the DB in Hive and their 
    tables. 

SHOW databases;

CREATE DATABASE FLIGHTS_DB;

USE FLIGHTS_DB;

SHOW TABLES;

Finally run the pipeline triggering the DAG manually. 

# To run pyspark in the console 
/home/hadoop/spark/bin/pyspark
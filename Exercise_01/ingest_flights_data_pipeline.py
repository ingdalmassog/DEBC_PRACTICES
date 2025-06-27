import os

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator 
#from airflow.providers.python.operators.external_python import ExternalPythonOperator
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

#from scripts import flights_data_tramsformation_and_load
#from scripts.flights_data_transformation_and_load import main_process

dag_name, _ = os.path.basename(os.path.realpath(__file__)).split('.')

args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id=f"{dag_name}",
    default_args=args,
    catchup=False,
    schedule_interval='0 6 * * 7', # Added only to keep the DAG automatic. 
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=30),
    tags=['ingest', 'transform', 'flights', 'data']
) as dag:

    start_process = EmptyOperator(
        task_id='start_process',
    )

    finish_process = EmptyOperator(
        task_id='finsh_process',
    )

    with TaskGroup(group_id='get_files') as get_files:
        get_airports_data = BashOperator(task_id='get_airports_data', bash_command="scripts/ingest_airports_data_script.sh")
        get_flights_data = BashOperator(task_id='get_flights_data', bash_command="scripts/ingest_flights_data.sh")
        
        [get_flights_data, get_airports_data]


    prepare_files_in_hdfs = BashOperator(
        task_id='prepare_files_in_hdfs',
        bash_command="/home/hadoop/hadoop/bin/hdfs dfs -chmod 777 hdfs://172.17.0.2:9000/ingest/*"
    )
    
    # show_files_in_hdfs = BashOperator(
    #     task_id='show_files_in_hdfs',
    #     bash_command="/home/hadoop/hadoop/bin/hdfs dfs -ls ingest/flights_data"
    # )

    transformation_and_load = BashOperator(
        task_id='transform_and_load',
        bash_command="""/home/hadoop/spark/bin/spark-submit\
                --files /home/hadoop/hive/conf/hive-site.xml\
                /home/hadoop/scripts/flights_data_transformation_and_load.py\
            """,
        )

    start_process >> get_files >> prepare_files_in_hdfs\
        >> transformation_and_load >> finish_process





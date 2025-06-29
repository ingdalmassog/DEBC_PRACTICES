import os

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.python import PythonOperator 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago



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
    tags=['ingest', 'transform', 'car', 'rental', 'data']
) as dag:

    dag.doc_md = f"""Main DAG to ingest car rental data in a Hive"""

    start_process = EmptyOperator(
        task_id='start_process',
    )

    finish_process = EmptyOperator(
        task_id='finsh_process',
    )

    with TaskGroup(group_id='get_files') as get_files:
        get_car_rental_data = BashOperator(task_id='get_car_rental_data', bash_command="scripts/ingest_car_rental_data.sh")
        get_geo_data = BashOperator(task_id='get_geo_data', bash_command="scripts/ingest_georef_data.sh")
        [get_car_rental_data, get_geo_data]


    prepare_files_in_hdfs = BashOperator(
        task_id='prepare_files_in_hdfs',
        bash_command="/home/hadoop/hadoop/bin/hdfs dfs -chmod 777 hdfs://172.17.0.2:9000/ingest/*"
    )
    
    # show_files_in_hdfs = BashOperator(
    #     task_id='show_files_in_hdfs',
    #     bash_command="/home/hadoop/hadoop/bin/hdfs dfs -ls hdfs://172.17.0.2:9000/ingest/*"
    # )
    
    trigger_transform_and_load_data = TriggerDagRunOperator(
            task_id = 'trigger_transform_and_load_data',
            trigger_dag_id= 'transform_and_load_car_rental_data',
            # wait_for_completion=True, --  not available for airflow 2.3.0
            allowed_states=['success'], 
            failed_states=['failed'],
            poke_interval=1,            # How often to check child status
            reset_dag_run=True           # Optional: clears existing dagrun if exists   
        )


    start_process >> get_files >> prepare_files_in_hdfs\
        >> trigger_transform_and_load_data >> finish_process





import os

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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
    schedule_interval=None,
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=30),
    tags=['ingest', 'transform', 'car', 'rental', 'data']
) as dag:
    
    dag.doc_md = f"""Sub DAG to transform the car rental data in HDFS and put in a Hive's table"""

    start_process = EmptyOperator(
        task_id='start_process',
    )

    finish_process = EmptyOperator(
        task_id='finsh_process',
    )

    transformation_and_load = BashOperator(
        task_id='transform_and_load',
        bash_command="""/home/hadoop/spark/bin/spark-submit\
                --files /home/hadoop/hive/conf/hive-site.xml\
                /home/hadoop/scripts/car_rental_data_transformation_and_load.py\
            """,
        )

    start_process >> transformation_and_load >> finish_process

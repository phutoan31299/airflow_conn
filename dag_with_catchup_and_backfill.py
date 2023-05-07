from datetime import datetime, timedelta

from airflow import DAG 
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'toan.dao',
    'retries': 5,
    'retry_relay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_catchup_backfill_v02',
    default_args=default_args,
    start_date=datetime(2023, 5, 1),
    schedule='@daily',
    catchup=False
    ) as dag:

    task1 = BashOperator(
        task_id = 'task1',
        bash_command='echo This is a simple bash command!'
    )

    
    
from airflow import DAG 

from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'toan.dao',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_sklearn():
    import sklearn
    print(f"scikit-learn with version: {sklearn.__version__}")

def get_matplotlib():
    import matplotlib
    print(f"matplotlib with version: {matplotlib.__version__} ")


with DAG(
    dag_id = 'our_dag_with_python_dependencies_v02',
    default_args=default_args,
    description='This is our first dag using python operator',
    start_date=datetime(2023, 5, 3, 2),
    schedule='@daily'
) as dag:

    get_sklearn = PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn
    )

    get_matplotlib = PythonOperator(
        task_id='get_matplotlib',
        python_callable=get_matplotlib
    )

    get_sklearn >> get_matplotlib
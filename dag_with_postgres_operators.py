from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'toan.dao',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_postgres_operator_v02",
    start_date=datetime(2023 ,5 ,6),
    schedule='0 0 * * *'
) as dag:

    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    task2 =PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ ds }}','{{ dag.dag_id }}')
        """
    )

    task1 >> task2




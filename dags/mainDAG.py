from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from utils.functions import fetch_university_data, process_university_data, load_to_postgres

default_args = {
    'owner': 'mark_ed',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'university_data_processing',
        default_args=default_args,
        description='Process university data and load into PostgreSQL',
        schedule_interval='0 3 * * *',
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_university_data',
        python_callable=fetch_university_data,
    )

    process_data_task = PythonOperator(
        task_id='process_university_data',
        python_callable=process_university_data,
        provide_context=True,
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
        op_kwargs={
            'postgres_db': 'bi_university_data',
            'postgres_user': 'airflow',
            'postgres_password': 'airflow',
            'postgres_host': 'postgres',
            'postgres_port': '5432',
        },
    )

    fetch_data_task >> process_data_task >> load_to_postgres_task

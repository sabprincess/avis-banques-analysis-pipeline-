from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='dbt_run_pipeline',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    description='DAG Airflow pour lancer DBT chaque semaine'
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt_command',
        bash_command="""
    source ~/dbt-env/bin/activate && \
    cd "//wsl.localhost/Ubuntu-20.04/home/sabrine123/avis_reviews_dbt" && \
    dbt run
    """,
    dag=dag,
    )

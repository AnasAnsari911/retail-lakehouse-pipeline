from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id = "hello_dag",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["learning"],
) as dag:
    
    say_hello = BashOperator(
        task_id = 'say_hello',
        bash_command = "echo 'Hello from Airflow'",
    )

    say_date = BashOperator(
        task_id = 'say_date',
        bash_command = "date",
    )

    say_hello >> say_date


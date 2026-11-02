from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Where your project is mounted inside the Airflow containers
PROJECT_DIR = "/opt/project"

# Settings applied to every task in this DAG
default_args = {
    "retries": 1,                         # if a task fails, try once more
    "retry_delay": timedelta(minutes=2),  # wait 2 minutes before retrying
}


def spark_task(task_id, module):
    """One task that runs a pipeline script, like: python -m src.spark.bronze"""
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && python -m {module}",
    )


with DAG(
    dag_id="retail_pipeline",
    description="Bronze -> Silver -> Gold -> PostgreSQL -> data quality checks",
    start_date=datetime(2026, 1, 1),
        schedule="@daily",      # runs once a day at midnight (UTC)
    catchup=False,
    default_args=default_args,
    tags=["retail", "etl"],
) as dag:

    bronze = spark_task("bronze", "src.spark.bronze")
    silver = spark_task("silver", "src.spark.silver")
    gold = spark_task("gold", "src.spark.gold")
    publish_gold = spark_task("publish_gold", "src.spark.publish_gold")
    quality_checks = spark_task("quality_checks", "src.quality.run_checks")

    bronze >> silver >> gold >> publish_gold >> quality_checks
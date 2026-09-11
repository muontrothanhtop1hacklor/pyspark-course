"""Orchestrate multi-source Bronze ingestion followed by Silver and Gold."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


# The image provides Python, PySpark, and Java. `which python` in the built
# image returned this path.
PYTHON_EXECUTABLE = "/home/airflow/.local/bin/python"
PROJECT_DIR = "/opt/airflow/exercises/02-orders-lakehouse"


def spark_command(script_name: str) -> str:
    return f'"{PYTHON_EXECUTABLE}" "{PROJECT_DIR}/{script_name}"'


with DAG(
    dag_id="orders_lakehouse_pipeline",
    description="Multi-source Bronze ingestion converging into Silver and Gold",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=(
        "Three Bronze source jobs run in parallel, then converge into Silver "
        "and Gold. The Airflow image includes PySpark and Java 17."
    ),
) as dag:
    with TaskGroup(group_id="bronze_ingestion") as bronze_ingestion:
        bronze_web_task = BashOperator(
            task_id="bronze_web_task",
            bash_command=spark_command("run_bronze_web.py"),
        )
        bronze_mobile_task = BashOperator(
            task_id="bronze_mobile_task",
            bash_command=spark_command("run_bronze_mobile.py"),
        )
        bronze_store_task = BashOperator(
            task_id="bronze_store_task",
            bash_command=spark_command("run_bronze_store.py"),
        )

    silver_task = BashOperator(
        task_id="silver_task",
        bash_command=spark_command("run_silver.py"),
    )
    gold_task = BashOperator(
        task_id="gold_task",
        bash_command=spark_command("run_gold.py"),
    )

    bronze_ingestion >> silver_task >> gold_task

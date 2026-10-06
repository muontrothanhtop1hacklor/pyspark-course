from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='medallion_scale_etl_dag',
    default_args=default_args,
    description='ETL DAG running Bronze, Silver, Gold on 1M/10M/100M scale',
    schedule_interval=None, # Run manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        "scale": "1m" # Default scale, user can trigger with 10m or 100m
    }
) as dag:
    
    # The base path inside the docker container
    base_dir = "/opt/airflow/exercises/17-medallion-airflow-scale"
    scale = "{{ params.scale }}"
    
    input_path = f"{base_dir}/{scale}/data/raw"
    lakehouse_path = f"{base_dir}/{scale}/data/lakehouse"

    run_bronze = BashOperator(
        task_id='run_bronze_layer',
        bash_command=f"python {base_dir}/scripts/etl_bronze.py --input {input_path} --output {lakehouse_path}"
    )

    run_silver = BashOperator(
        task_id='run_silver_layer',
        bash_command=f"python {base_dir}/scripts/etl_silver.py --lakehouse {lakehouse_path}"
    )

    run_gold = BashOperator(
        task_id='run_gold_layer',
        bash_command=f"python {base_dir}/scripts/etl_gold.py --lakehouse {lakehouse_path}"
    )

    run_bronze >> run_silver >> run_gold

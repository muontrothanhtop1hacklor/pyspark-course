from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Environment configuration - Cần sửa lại PYTHON_EXECUTABLE và PROJECT_DIR
# phù hợp với môi trường triển khai thực tế trên Airflow / Docker
# Ở đây ta dùng lệnh python hệ thống vì PySpark đã được cài vào môi trường này.
PYTHON_EXECUTABLE = "python"
PROJECT_DIR = "/opt/airflow/exercises/19-iceberg-nessie-catalog"

def spark_command(script_name: str) -> str:
    """Tạo bash command để chạy script."""
    return f'"{PYTHON_EXECUTABLE}" "{PROJECT_DIR}/{script_name}"'

with DAG(
    dag_id="nessie_iceberg_git_for_data",
    description="Pipeline chạy giả lập và kiểm tra tính năng Git-for-data (Nessie & Iceberg)",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["iceberg", "nessie", "lakehouse"],
    doc_md=(
        "DAG này lên lịch chạy các PySpark scripts tương tác với Nessie Catalog:\n"
        "1. Tải dữ liệu vào bảng Iceberg qua Nessie (branch main)\n"
        "2. Kiểm tra các thao tác rẽ nhánh (branching), sửa đổi dữ liệu (delete), "
        "   đánh tag (tagging) và so sánh dữ liệu giữa các nhánh."
    ),
) as dag:

    # Task 1: Load Data
    # Chạy script 01_load_data.py
    load_data_task = BashOperator(
        task_id="load_data_into_iceberg",
        bash_command=spark_command("01_load_data.py"),
    )

    # Task 2: Git for Data (Branch, Delete, Tag)
    # Chạy script 02_nessie_git_for_data.py
    git_for_data_task = BashOperator(
        task_id="test_nessie_git_for_data",
        bash_command=spark_command("02_nessie_git_for_data.py"),
    )

    # Định nghĩa luồng phụ thuộc: Load data phải xong thì mới chạy kiểm tra Git for Data
    load_data_task >> git_for_data_task

"""Hands-on scaffold for the Orders Data Lakehouse exercise.

Complete each TODO using README-practice.md before comparing with the reference
implementation in spark_orders_lakehouse.py.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("province", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("order_date", StringType(), True),
    ]
)


def read_orders(spark: SparkSession, input_path: Path) -> DataFrame:
    """Read the source CSV with a raw schema."""
    df=spark.read.csv(str(input_path), header=True, schema=RAW_SCHEMA)
    return df


def build_bronze(raw: DataFrame, source_file: str, load_time: str) -> DataFrame:
    """Add ingestion metadata while preserving raw source columns."""
    # TODO: add source_file, load_time, and ingest_seq.
    
    raise NotImplementedError


def build_silver(bronze: DataFrame) -> DataFrame:
    """Clean, cast, normalize, filter, and deduplicate Bronze records."""
    # TODO:
    # 1. Trim string columns.
    # 2. Cast order_id and amount safely.
    # 3. Normalize customer_id, province, and status.
    # 4. Parse order_date as yyyy-MM-dd.
    # 5. Keep only valid records with amount > 0.
    # 6. Deduplicate by order_id using ingest_seq.
    raise NotImplementedError


def build_gold(silver: DataFrame) -> DataFrame:
    """Aggregate valid Silver orders by province."""
    # TODO: calculate total_orders, total_amount, success_orders,
    # and failed_orders, then order by province.
    raise NotImplementedError


def write_layer(dataframe: DataFrame, output_path: Path) -> None:
    """Write a DataFrame as a header-enabled Spark CSV directory."""
    # TODO: use overwrite mode and header=True.
    raise NotImplementedError


def upload_directory_to_minio(
    local_dir: Path,
    bucket: str,
    prefix: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    """Upload every file below local_dir to an S3-compatible MinIO bucket."""
    # TODO:
    # 1. Create a boto3 S3 client.
    # 2. Create the bucket if it does not exist.
    # 3. Recursively upload files using prefix/object-name.
    raise NotImplementedError


def main() -> None:
    project_dir = Path(__file__).resolve().parent
    input_path = project_dir / "orders.csv"
    output_dir = project_dir / "output" / "lakehouse"

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password123")
    bucket = "lakehouse-demo"
    load_time = datetime.now(timezone.utc).isoformat()

    spark = (
        SparkSession.builder.appName("PracticeOrdersLakehouse")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # TODO: implement the complete flow in this order:
        # raw -> bronze -> write Bronze -> silver -> write Silver
        # -> gold -> write Gold -> upload all three layers.
        raw = read_orders(spark, input_path)
        bronze = build_bronze(raw, input_path.name, load_time)
        silver = build_silver(bronze)
        gold = build_gold(silver)

        bronze.show(truncate=False)
        silver.show(truncate=False)
        gold.show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

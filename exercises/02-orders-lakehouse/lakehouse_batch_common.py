"""Shared Spark setup and layer output helpers for the multi-source batch flow."""

import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id

# The unchanged legacy module imports try_to_date although its Silver logic
# uses try_to_timestamp. PySpark 4.0.1 has no try_to_date, so provide the
# unused symbol only long enough for that module import to succeed.
import pyspark.sql.functions as spark_functions

if not hasattr(spark_functions, "try_to_date"):
    spark_functions.try_to_date = spark_functions.to_date

from spark_orders_lakehouse import RAW_SCHEMA, upload_directory_to_minio


PROJECT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_DIR / "output"
BUCKET = "lakehouse-demo"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def run_bronze(channel: str) -> None:
    """Ingest one static source into CSV, Parquet, and MinIO."""
    input_path = PROJECT_DIR / f"orders_{channel}.csv"
    csv_path = OUTPUT_DIR / "lakehouse" / "bronze" / channel
    parquet_path = OUTPUT_DIR / "bronze" / channel

    spark = create_spark(f"OrdersBronze{channel.title()}")
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.option("header", True).schema(RAW_SCHEMA).csv(str(input_path))
        bronze = (
            raw.withColumn("source_file", lit(input_path.name))
            .withColumn("source_channel", lit(channel))
            .withColumn("ingest_seq", monotonically_increasing_id())
        )
        bronze.write.mode("overwrite").option("header", True).csv(str(csv_path))
        bronze.write.mode("overwrite").parquet(str(parquet_path))
        upload_directory_to_minio(
            csv_path,
            BUCKET,
            f"bronze/{channel}",
            MINIO_ENDPOINT,
            MINIO_ACCESS_KEY,
            MINIO_SECRET_KEY,
        )
        print(f"Bronze {channel} completed: {csv_path}")
    finally:
        spark.stop()

"""Write the raw Bronze DataFrame to an Iceberg table managed by Nessie."""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StructType


def require_spark_35() -> None:
    """Fail early when this Spark 3.5-only job uses another PySpark version."""
    import pyspark

    version = tuple(int(part) for part in pyspark.__version__.split(".")[:2])
    if version != (3, 5):
        raise RuntimeError(
            "This Iceberg job requires PySpark 3.5.x, but found "
            f"{pyspark.__version__}. Run it with the "
            "'pyspark_iceberg_357' environment."
        )


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


def main() -> None:
    require_spark_35()
    project_dir = Path(__file__).resolve().parent
    input_path = project_dir / "orders.csv"
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password123")
    nessie_uri = os.getenv("NESSIE_URI", "http://localhost:19120/api/v2")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse")

    spark = (
        SparkSession.builder
        .appName("OrdersIcebergBronze")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0",
                    "org.apache.iceberg:iceberg-nessie:1.11.0",
                    "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
                ]
            ),
        )
        .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.nessie.uri", nessie_uri)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", warehouse)
        .config(
            "spark.sql.catalog.nessie.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.nessie.s3.endpoint", endpoint)
        .config("spark.sql.catalog.nessie.s3.region", "us-east-1")
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id", access_key)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", secret_key)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        load_time = datetime.now(timezone.utc).isoformat()
        raw = spark.read.option("header", True).schema(RAW_SCHEMA).csv(str(input_path))
        bronze = (
            raw.withColumn("source_file", lit(input_path.name))
            .withColumn("load_time", lit(load_time))
        )

        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.db")
        bronze.writeTo("nessie.db.orders_bronze").using("iceberg").createOrReplace()
        print("Iceberg Bronze table written: nessie.db.orders_bronze")
        bronze.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

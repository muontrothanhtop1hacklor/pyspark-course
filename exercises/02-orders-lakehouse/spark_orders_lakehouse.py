"""PySpark Bronze/Silver/Gold flow with local output uploaded to MinIO."""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import functools

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import boto3
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, lit, sum as spark_sum, upper, trim, try_to_date
from pyspark.sql.types import StringType, StructField, StructType


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
 
# Bản đồ chuẩn hoá province: key đã lower + trim, value là tên chuẩn muốn dùng
PROVINCE_MAP = {
    "hanoi": "Hanoi",
    "ha noi": "Hanoi",
    "hn": "Hanoi",
    "da nang": "Da Nang",
    "danang": "Da Nang",
    "ho chi minh": "Ho Chi Minh",
    "tp. ho chi minh": "Ho Chi Minh",
    "tp ho chi minh": "Ho Chi Minh",
    "sai gon": "Ho Chi Minh",
    "hồ chí minh": "Ho Chi Minh",
    "can tho": "Can Tho",
}
 
# Bản đồ sửa lỗi chính tả status (key đã upper + trim)
STATUS_MAP = {
    "SUCCES": "SUCCESS",
    "FAIL": "FAILED",
    "CANCELED": "CANCELLED",
}
 
 
def _map_column(col_expr, mapping: dict, default_expr):
    """Dựng chuỗi khi/otherwise từ 1 dict Python -> biểu thức Spark khi/otherwise.
    mapping: {key_da_chuan_hoa: gia_tri_muon_gan}
    default_expr: giá trị trả về nếu không khớp key nào trong mapping
    """
    return functools.reduce(
        lambda acc, kv: F.when(col_expr == F.lit(kv[0]), F.lit(kv[1])).otherwise(acc),
        mapping.items(),
        default_expr,
    )
 
 
def build_silver(bronze: DataFrame) -> DataFrame:
    """Clean raw Bronze records and enforce Silver column types.
 
    Xử lý các lỗi thực tế đã gặp trong dữ liệu:
      - order_id thiếu / không phải số           -> loại
      - customer_id viết thường, có dấu gạch ngang -> chuẩn hoá upper, bỏ '-'
      - province viết tắt/khác cách viết          -> map về 1 trong 4 tên chuẩn
      - amount: dấu phẩy ("1,250,000"), hậu tố k
        ("200k"), khoảng trắng, chuỗi rác
        ("NULL","NaN","abc")                      -> parse về decimal, rác -> NULL
      - amount <= 0                               -> loại
      - status viết hoa/thường lẫn lộn, sai
        chính tả (SUCCES/FAIL/CANCELED)           -> chuẩn hoá + map lỗi chính tả
      - order_date thiếu / không parse được
        (vd 'bad-date')                           -> loại (dùng try_to_timestamp + cast date,
                                                     không dùng to_date để tránh
                                                     Spark raise exception khi ANSI mode bật)
      - order_id trùng lặp                        -> giữ bản ghi nạp vào Bronze sớm nhất
    """
    trimmed = bronze.select(
        F.trim(F.col("order_id")).alias("order_id_raw"),
        F.trim(F.col("customer_id")).alias("customer_id_raw"),
        F.trim(F.col("province")).alias("province_raw"),
        F.trim(F.col("amount")).alias("amount_raw"),
        F.trim(F.col("status")).alias("status_raw"),
        F.trim(F.col("order_date")).alias("order_date_raw"),
        F.col("ingest_seq"),  # cần cột này để dedup ổn định — xem ghi chú cuối file
    )
 
    # --- amount: bỏ dấu phẩy ngăn cách hàng nghìn, xử lý hậu tố k/K ---
    amount_no_comma = F.regexp_replace(F.col("amount_raw"), ",", "")
    is_k_suffix = F.upper(amount_no_comma).rlike(r"^-?\d+(\.\d+)?K$")
    amount_digits_only = F.expr("substring(amount_no_comma, 1, length(amount_no_comma) - 1)")
 
    cleaned = (
        trimmed
        .withColumn("amount_no_comma", amount_no_comma)
        .withColumn(
            "amount",
            F.when(
                is_k_suffix,
                F.expr("try_cast(substring(amount_no_comma, 1, length(amount_no_comma) - 1) as decimal(18,2))") * 1000,
            ).otherwise(
                F.expr("try_cast(amount_no_comma as decimal(18,2))")
            ),
        )
        .withColumn("order_id", F.expr("try_cast(order_id_raw as long)"))
        .withColumn(
            "customer_id",
            F.upper(F.regexp_replace(F.col("customer_id_raw"), "-", "")),
        )
        .withColumn(
            "province",
            _map_column(F.lower(F.col("province_raw")), PROVINCE_MAP, F.col("province_raw")),
        )
        .withColumn(
            "status",
            _map_column(F.upper(F.col("status_raw")), STATUS_MAP, F.upper(F.col("status_raw"))),
        )
        # Dùng try_to_timestamp thay vì try_to_date: try_to_date chỉ có từ Spark 4.1+,
        # còn try_to_timestamp đã có từ Spark 4.0 (Spark 3.x thì dùng to_timestamp bình
        # thường vì ANSI mode mặc định tắt, không raise exception khi parse lỗi).
        # Chuỗi không parse được ('bad-date') -> NULL thay vì Spark ném exception.
        .withColumn(
            "order_date",
            F.expr("CAST(try_to_timestamp(order_date_raw, 'yyyy-MM-dd') AS DATE)"),
        )
        .drop("amount_no_comma", "order_id_raw", "customer_id_raw", "province_raw",
              "amount_raw", "status_raw", "order_date_raw")
    )
 
    filtered = cleaned.filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("province").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
        & F.col("order_date").isNotNull()
    )
 
    # Khử trùng order_id: giữ bản ghi có ingest_seq nhỏ nhất (nạp vào Bronze sớm nhất)
    window = Window.partitionBy("order_id").orderBy(F.col("ingest_seq").asc())
    deduped = (
        filtered
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "ingest_seq")
    )
 
    return deduped
 
 
def build_gold(silver: DataFrame) -> DataFrame:
    """Aggregate Silver orders by province for the Gold layer."""
    return (
        silver.groupBy("province")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("amount").alias("total_amount"),
            F.sum((F.col("status") == "SUCCESS").cast("int")).alias("success_orders"),
            F.sum((F.col("status") == "FAILED").cast("int")).alias("failed_orders"),
        )
        .orderBy("province")
    )

def upload_directory_to_minio(
    local_dir: Path, bucket: str, prefix: str, endpoint: str, access_key: str, secret_key: str
) -> None:
    """Create the bucket when needed and upload every Spark part-file."""
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    existing = {item["Name"] for item in client.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)

    for file_path in local_dir.rglob("*"):
        if file_path.is_file():
            object_key = f"{prefix}/{file_path.relative_to(local_dir).as_posix()}"
            client.upload_file(str(file_path), bucket, object_key)
            print(f"Uploaded s3://{bucket}/{object_key}")


def main() -> None:
    project_dir = Path(__file__).resolve().parent
    input_path = project_dir / "orders.csv"
    output_dir = project_dir / "output" / "lakehouse"
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    nessie_uri = os.getenv("NESSIE_URI", "http://localhost:19120/api/v2")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password123")
    bucket = "lakehouse-demo"
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse")
    load_time = datetime.now(timezone.utc).isoformat()

    spark = (
        SparkSession.builder
        .appName("OrdersLakehouseExercise")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
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
        .config("spark.sql.catalog.nessie.warehouse", iceberg_warehouse)
        .config(
            "spark.sql.catalog.nessie.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.nessie.s3.endpoint", endpoint)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id", access_key)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", secret_key)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = spark.read.option("header", True).schema(RAW_SCHEMA).csv(str(input_path))
        bronze = (
                raw.withColumn("source_file", lit(input_path.name))
                .withColumn("load_time", lit(load_time))
                .withColumn("ingest_seq", F.monotonically_increasing_id())   # ← THÊM DÒNG NÀY
                )
        bronze_path = output_dir / "bronze" / "orders"
        silver_path = output_dir / "silver" / "orders"
        gold_path = output_dir / "gold" / "order_summary"

        bronze.write.mode("overwrite").option("header", True).csv(str(bronze_path))
        silver = build_silver(bronze)
        silver.write.mode("overwrite").option("header", True).csv(str(silver_path))
        gold = build_gold(silver)
        gold.write.mode("overwrite").option("header", True).csv(str(gold_path))

        print("=== Bronze: raw data ===")
        bronze.show(truncate=False)
        print("=== Silver: cleaned data ===")
        silver.show(truncate=False)
        print("=== Gold: order summary ===")
        gold.show()

        upload_directory_to_minio(
            bronze_path, bucket, "bronze/orders", endpoint, access_key, secret_key
        )
        try:
            (
                bronze.writeTo("nessie.db.orders_bronze")
                .using("iceberg")
                .createOrReplace()
            )
            print("Iceberg Bronze table written: nessie.db.orders_bronze")
        except Exception as iceberg_error:
            print(
                "WARNING: Iceberg Bronze write failed; "
                f"CSV/MinIO output remains available: {iceberg_error}"
            )
        upload_directory_to_minio(
            silver_path, bucket, "silver/orders", endpoint, access_key, secret_key
        )
        upload_directory_to_minio(
            gold_path, bucket, "gold/order_summary", endpoint, access_key, secret_key
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

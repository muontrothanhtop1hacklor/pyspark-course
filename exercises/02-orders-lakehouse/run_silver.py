"""Union all Bronze Parquet sources and write the shared Silver layer."""

from lakehouse_batch_common import (
    BUCKET,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    OUTPUT_DIR,
    create_spark,
)
from spark_orders_lakehouse import build_silver, upload_directory_to_minio


if __name__ == "__main__":
    spark = create_spark("OrdersSilver")
    spark.sparkContext.setLogLevel("WARN")
    try:
        bronze_paths = [str(OUTPUT_DIR / "bronze" / channel) for channel in ("web", "mobile", "store")]
        bronze = spark.read.parquet(*bronze_paths)
        silver = build_silver(bronze)
        csv_path = OUTPUT_DIR / "lakehouse" / "silver" / "orders"
        parquet_path = OUTPUT_DIR / "silver"
        silver.write.mode("overwrite").option("header", True).csv(str(csv_path))
        silver.write.mode("overwrite").parquet(str(parquet_path))
        upload_directory_to_minio(
            csv_path,
            BUCKET,
            "silver/orders",
            MINIO_ENDPOINT,
            MINIO_ACCESS_KEY,
            MINIO_SECRET_KEY,
        )
        print(f"Silver completed: {csv_path}")
    finally:
        spark.stop()

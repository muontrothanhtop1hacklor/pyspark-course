"""Read the shared Silver Parquet layer and write Gold aggregates."""

from lakehouse_batch_common import (
    BUCKET,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    OUTPUT_DIR,
    create_spark,
)
from spark_orders_lakehouse import build_gold, upload_directory_to_minio


if __name__ == "__main__":
    spark = create_spark("OrdersGold")
    spark.sparkContext.setLogLevel("WARN")
    try:
        silver = spark.read.parquet(str(OUTPUT_DIR / "silver"))
        gold = build_gold(silver)
        csv_path = OUTPUT_DIR / "lakehouse" / "gold" / "order_summary"
        gold.write.mode("overwrite").option("header", True).csv(str(csv_path))
        upload_directory_to_minio(
            csv_path,
            BUCKET,
            "gold/order_summary",
            MINIO_ENDPOINT,
            MINIO_ACCESS_KEY,
            MINIO_SECRET_KEY,
        )
        print(f"Gold completed: {csv_path}")
    finally:
        spark.stop()

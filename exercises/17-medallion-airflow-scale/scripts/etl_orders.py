import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def get_paths(scale: str):
    base_dir = f"c:/Users/Administrator/spark introduce learn/files (5)/{scale}/data"
    return os.path.join(base_dir, "raw"), os.path.join(base_dir, "lakehouse")

def build_orders_etl(spark: SparkSession, input_dir: str, output_dir: str, scale: str):
    """
    ETL job cho dữ liệu Orders tuân theo kiến trúc Medallion.
    """
    # 1. BRONZE LAYER
    print(f"=== [BRONZE] Xử lý lớp dữ liệu thô (Raw) cho {scale} ===")
    df_raw = spark.read.parquet(input_dir)
    bronze_out = os.path.join(output_dir, "bronze", "orders")
    df_raw.write.mode("overwrite").parquet(bronze_out)
    print(f"Đã lưu Bronze layer tại: {bronze_out}")
    
    # 2. SILVER LAYER
    print(f"=== [SILVER] Xử lý lớp dữ liệu sạch (Cleansed) cho {scale} ===")
    df_bronze = spark.read.parquet(bronze_out)
    df_silver = df_bronze \
        .withColumn("order_year", F.year("order_date")) \
        .withColumn("order_month", F.month("order_date")) \
        .fillna({"rating": 3, "coupon_code": "NO_COUPON", "notes": "no_note"})
        
    silver_out = os.path.join(output_dir, "silver", "orders")
    df_silver.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(silver_out)
    print(f"Đã lưu Silver layer tại: {silver_out}")

    # 3. GOLD LAYER
    print(f"=== [GOLD] Xử lý lớp dữ liệu phân tích (Aggregated) cho {scale} ===")
    df_cleansed = spark.read.parquet(silver_out)
    gold_dir = os.path.join(output_dir, "gold")
    
    df_customer = df_cleansed.groupBy("customer_id", "country", "city", "gender", "customer_segment").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("lifetime_value"),
        F.avg("rating").alias("avg_rating"),
        F.sum(F.when(F.col("return_flag") == True, 1).otherwise(0)).alias("total_returns")
    )
    gold_cust_out = os.path.join(gold_dir, "customer_360")
    df_customer.write.mode("overwrite").parquet(gold_cust_out)
    print(f" Đã lưu Gold (Customer 360) tại: {gold_cust_out}")
    
    df_category = df_cleansed.filter(F.col("status") != "cancelled").groupBy("order_year", "order_month", "category").agg(
        F.sum("quantity").alias("total_items_sold"),
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_buyers")
    )
    gold_cat_out = os.path.join(gold_dir, "category_performance")
    df_category.write.mode("overwrite").parquet(gold_cat_out)
    print(f" Đã lưu Gold (Category Performance) tại: {gold_cat_out}")

    df_shipping = df_cleansed.filter(F.col("status") == "shipped").groupBy("country").agg(
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.max("delivery_days").alias("max_delivery_days"),
        F.avg("shipping_fee").alias("avg_shipping_fee")
    )
    gold_ship_out = os.path.join(gold_dir, "shipping_performance")
    df_shipping.write.mode("overwrite").parquet(gold_ship_out)
    print(f" Đã lưu Gold (Shipping Performance) tại: {gold_ship_out}")
    
    print(f"Hoàn tất toàn bộ quy trình ETL Medallion Architecture cho tập {scale}!")

def main():
    parser = argparse.ArgumentParser(description="Medallion ETL Process")
    parser.add_argument("--scale", type=str, choices=["1m", "10m", "100m"], default="1m", help="Quy mô dữ liệu (1m, 10m, 100m)")
    args = parser.parse_args()
    
    scale_configs = {
        "1m": {"memory": "4g", "partitions": "10"},
        "10m": {"memory": "4g", "partitions": "50"},
        "100m": {"memory": "8g", "partitions": "200"}
    }
    cfg = scale_configs[args.scale]
    
    input_dir, output_dir = get_paths(args.scale)
    
    spark = SparkSession.builder \
        .appName(f"Medallion_ETL_ORDERS_{args.scale.upper()}") \
        .config("spark.driver.memory", cfg["memory"]) \
        .config("spark.executor.memory", cfg["memory"]) \
        .config("spark.sql.shuffle.partitions", cfg["partitions"]) \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        build_orders_etl(spark, input_dir, output_dir, args.scale)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

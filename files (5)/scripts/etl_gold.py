import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def run_gold(spark: SparkSession, input_dir: str, output_dir: str):
    print("=== [GOLD] Aggregations & Analytics ===")
    silver_in = os.path.join(input_dir, "silver", "orders")
    df_cleansed = spark.read.parquet(silver_in)
    gold_dir = os.path.join(output_dir, "gold")
    
    df_customer = df_cleansed.groupBy("customer_id", "country", "city", "gender", "customer_segment").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("lifetime_value"),
        F.avg("rating").alias("avg_rating"),
        F.sum(F.when(F.col("return_flag") == True, 1).otherwise(0)).alias("total_returns")
    )
    gold_cust_out = os.path.join(gold_dir, "customer_360")
    df_customer.write.mode("overwrite").parquet(gold_cust_out)
    print(f"Đã lưu Gold (Customer 360) tại: {gold_cust_out}")
    
    df_category = df_cleansed.filter(F.col("status") != "cancelled").groupBy("order_year", "order_month", "category").agg(
        F.sum("quantity").alias("total_items_sold"),
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_buyers")
    )
    gold_cat_out = os.path.join(gold_dir, "category_performance")
    df_category.write.mode("overwrite").parquet(gold_cat_out)
    print(f"Đã lưu Gold (Category Performance) tại: {gold_cat_out}")

    df_shipping = df_cleansed.filter(F.col("status") == "shipped").groupBy("country").agg(
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.max("delivery_days").alias("max_delivery_days"),
        F.avg("shipping_fee").alias("avg_shipping_fee")
    )
    gold_ship_out = os.path.join(gold_dir, "shipping_performance")
    df_shipping.write.mode("overwrite").parquet(gold_ship_out)
    print(f"Đã lưu Gold (Shipping Performance) tại: {gold_ship_out}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lakehouse", type=str, default="c:/Users/Administrator/spark introduce learn/files (5)/1m/data/lakehouse")
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("ETL_GOLD") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_gold(spark, args.lakehouse, args.lakehouse)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

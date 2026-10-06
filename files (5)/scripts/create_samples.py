import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def make_dirty(df, dup_frac=0.01, err_frac=0.01):
    # 1. Randomly inject bad values (in-place mutation)
    df_dirty = df.withColumn(
        "total_amount",
        F.when(F.rand(seed=1) < err_frac, F.lit(-500.0)).otherwise(F.col("total_amount"))
    ).withColumn(
        "quantity",
        F.when(F.rand(seed=2) < err_frac, F.lit(0)).otherwise(F.col("quantity"))
    ).withColumn(
        "order_date",
        F.when(F.rand(seed=3) < err_frac, F.lit(None).cast("date")).otherwise(F.col("order_date"))
    ).withColumn(
        "status",
        F.when(F.rand(seed=4) < err_frac, F.lit("   lowercase_status ")).otherwise(F.col("status"))
    )
    
    # 2. Add duplicates to test Window deduplication
    df_dup = df_dirty.sample(fraction=dup_frac, seed=42) \
                     .withColumn("order_timestamp", F.col("order_timestamp") + F.expr("INTERVAL 1 DAYS")) \
                     .withColumn("status", F.lit("UPDATED_LATE"))
                     
    return df_dirty.unionByName(df_dup)

def main():
    base = "c:/Users/Administrator/spark introduce learn/files (5)"
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default=f"{base}/100m/data/raw")
    parser.add_argument("--out1m", type=str, default=f"{base}/1m/data/raw")
    parser.add_argument("--out10m", type=str, default=f"{base}/10m/data/raw")
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("CREATE_SAMPLES") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    print("Reading 100M data...")
    df_100m = spark.read.parquet(args.input)
    
    print("Generating 1M random sample & INJECTING DIRTY DATA...")
    df_1m = df_100m.sample(withReplacement=False, fraction=0.01, seed=42)
    df_1m_dirty = make_dirty(df_1m, dup_frac=0.02, err_frac=0.01) # 2% dups, 1% errors
    df_1m_dirty.write.mode("overwrite").parquet(args.out1m)
    print(f"Saved 1M dirty sample to {args.out1m}")
    
    print("Generating 10M random sample & INJECTING DIRTY DATA...")
    df_10m = df_100m.sample(withReplacement=False, fraction=0.1, seed=42)
    df_10m_dirty = make_dirty(df_10m, dup_frac=0.02, err_frac=0.01)
    df_10m_dirty.write.mode("overwrite").parquet(args.out10m)
    print(f"Saved 10M dirty sample to {args.out10m}")
    
    spark.stop()

if __name__ == "__main__":
    main()

import os
import time
import argparse
from pyspark.sql import SparkSession
from etl_bronze import run_bronze
from etl_silver import run_silver
from etl_gold import run_gold

def get_paths(scale: str):
    base_dir = f"c:/Users/Administrator/spark introduce learn/files (5)/{scale}/data"
    return os.path.join(base_dir, "raw"), os.path.join(base_dir, "lakehouse")

def benchmark(spark, scale_name, raw_input, lakehouse_output):
    print(f"\n{'='*50}")
    print(f"BENCHMARKING SCALE: {scale_name.upper()}")
    print(f"{'='*50}")
    
    # BRONZE
    t0 = time.time()
    run_bronze(spark, raw_input, lakehouse_output)
    bronze_time = time.time() - t0
    
    # SILVER
    t1 = time.time()
    run_silver(spark, lakehouse_output, lakehouse_output)
    silver_time = time.time() - t1
    
    # GOLD
    t2 = time.time()
    run_gold(spark, lakehouse_output, lakehouse_output)
    gold_time = time.time() - t2
    
    total_time = bronze_time + silver_time + gold_time
    
    print(f"\n--- {scale_name.upper()} RESULTS ---")
    print(f"Bronze : {bronze_time:.2f} s")
    print(f"Silver : {silver_time:.2f} s")
    print(f"Gold   : {gold_time:.2f} s")
    print(f"Total  : {total_time:.2f} s\n")
    
    return {
        "scale": scale_name.upper(),
        "bronze": bronze_time,
        "silver": silver_time,
        "gold": gold_time,
        "total": total_time
    }

def main():
    parser = argparse.ArgumentParser(description="Medallion ETL Benchmark")
    parser.add_argument("--scales", type=str, nargs="+", default=["1m", "10m"], help="Các quy mô cần benchmark (1m, 10m, 100m)")
    args = parser.parse_args()
    
    spark = SparkSession.builder \
        .appName("BENCHMARK_ETL") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    results = []
    try:
        for scale in args.scales:
            raw, lakehouse = get_paths(scale)
            res = benchmark(spark, scale, raw, lakehouse)
            results.append(res)
    finally:
        spark.stop()
        
    print("\n" + "*"*40)
    print("FINAL BENCHMARK REPORT")
    print("*"*40)
    for r in results:
        print(f"[{r['scale']}] Total: {r['total']:.2f}s (B: {r['bronze']:.1f}s, S: {r['silver']:.1f}s, G: {r['gold']:.1f}s)")

if __name__ == "__main__":
    main()

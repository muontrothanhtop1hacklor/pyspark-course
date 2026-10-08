import os
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
from pyspark.sql import SparkSession

def get_nessie_spark_session():
    iceberg_spark_scala_version = "3.5_2.12"
    iceberg_version = "1.5.0"
    nessie_version = "0.77.1"
    aws_version = "2.24.13"

    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-{iceberg_spark_scala_version}:{iceberg_version}",
        f"org.projectnessie.nessie-integrations:nessie-spark-extensions-{iceberg_spark_scala_version}:{nessie_version}",
        f"software.amazon.awssdk:bundle:{aws_version}",
        f"software.amazon.awssdk:url-connection-client:{aws_version}"
    ]

    nessie_uri = os.getenv("NESSIE_URI", "http://localhost:19120/api/v1")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

    builder = SparkSession.builder \
        .appName("IcebergNessieGit") \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", nessie_uri) \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/") \
        .config("spark.sql.catalog.nessie.s3.endpoint", minio_endpoint) \
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true") \
        .config("spark.sql.catalog.nessie.client.region", "us-east-1") \
        .config("spark.sql.catalog.nessie.s3.access-key-id", "admin") \
        .config("spark.sql.catalog.nessie.s3.secret-access-key", "password123") \
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.nessie.s3.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Use local jars if present (Airflow container), else resolve from Maven
    if os.path.exists("/opt/spark-jars"):
        import glob
        builder = builder.config("spark.jars", ",".join(sorted(glob.glob("/opt/spark-jars/*.jar"))))
    else:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    spark = builder.getOrCreate()
    
    return spark

if __name__ == "__main__":
    spark = get_nessie_spark_session()
    
    print("=== TRUY VAN NHANH CHINH (main) ===")
    try:
        count_main = spark.sql("SELECT COUNT(*) FROM nessie.bhxh.detail_1m").collect()[0][0]
        print(f"So luong ban ghi tren nhanh 'main': {count_main}")
    except Exception as e:
        print("Loi: Khong tim thay bang, vui long chay 01_load_data.py truoc.")
        spark.stop()
        exit(1)
        
    print("\n=== TAO NHANH MOI (experiment) ===")
    spark.sql("CREATE BRANCH IF NOT EXISTS experiment IN nessie")
    print("Da tao nhanh 'experiment'")
    
    # Chuyển đổi context sang nhánh experiment
    spark.sql("USE REFERENCE experiment IN nessie")
    
    print("\n=== XOA MOT SO DU LIEU TREN NHANH EXPERIMENT ===")
    # Xoá thử một số dữ liệu (ví dụ: các dòng có loai_kcb = 1)
    print("Dang thuc hien lenh DELETE tren nhanh experiment...")
    spark.sql("DELETE FROM nessie.bhxh.detail_1m WHERE MA_TINH = '01'")
    
    count_exp = spark.sql("SELECT COUNT(*) FROM nessie.bhxh.detail_1m").collect()[0][0]
    print(f"So luong ban ghi tren nhanh 'experiment' (sau khi xoa): {count_exp}")
    
    print("\n=== SO SANH GIUA 2 NHANH ===")
    spark.sql("USE REFERENCE main IN nessie")
    count_main_again = spark.sql("SELECT COUNT(*) FROM nessie.bhxh.detail_1m").collect()[0][0]
    print(f"So luong ban ghi tren nhanh 'main' van giu nguyen: {count_main_again}")
    print(f"Do lech: {count_main_again - count_exp} ban ghi.")
    
    print("\n=== TAO TAG ===")
    spark.sql("CREATE TAG IF NOT EXISTS snapshot_v1 IN nessie")
    print("Da tao tag 'snapshot_v1' tro toi trang thai hien tai cua nhanh 'main'.")
    
    spark.stop()

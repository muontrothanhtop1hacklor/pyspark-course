import os
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
from pyspark.sql import SparkSession

def get_nessie_spark_session():
    # Cấu hình các gói cần thiết: Iceberg, AWS SDK, Nessie
    iceberg_spark_scala_version = "3.5_2.12"
    iceberg_version = "1.5.0"
    nessie_version = "0.77.1"
    aws_version = "2.24.13"

    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-{iceberg_spark_scala_version}:{iceberg_version}",
        f"org.projectnessie.nessie-integrations:nessie-spark-extensions-{iceberg_spark_scala_version}:{nessie_version}",
        f"software.amazon.awssdk:bundle:{aws_version}",
        f"software.amazon.awssdk:url-connection-client:{aws_version}",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    nessie_uri = os.getenv("NESSIE_URI", "http://localhost:19120/api/v1")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

    builder = SparkSession.builder \
        .appName("IcebergNessieLoad") \
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
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.sql.catalog.nessie.s3.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
    # Check if local jars exist (in Airflow container)
    if os.path.exists("/opt/spark-jars"):
        import glob
        builder = builder.config("spark.jars", ",".join(sorted(glob.glob("/opt/spark-jars/*.jar"))))
    else:
        builder = builder.config("spark.jars.packages", ",".join(packages))
        
    spark = builder.getOrCreate()
    
    return spark

if __name__ == "__main__":
    spark = get_nessie_spark_session()
    
    # Tạo namespace (database) trong Nessie
    print("Tao namespace 'bhxh' trong Nessie Catalog...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bhxh")
    
    # Base path của thư mục sinh dữ liệu
    base_data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "synthetic_bhxh", "output"))
    
    scales = ["1M", "10M", "100M"]
    
    for scale in scales:
        detail_path = os.path.join(base_data_path, scale, "DETAIL")
        table_name = f"nessie.bhxh.detail_{scale.lower()}"
        
        # Kiểm tra xem thư mục có tồn tại không
        if not os.path.exists(detail_path):
            print(f"Canh bao: Khong tim thay du lieu tai {detail_path}. Bo qua {scale}.")
            continue
            
        print(f"Dang xu ly du lieu {scale}...")
        
        # Đọc dữ liệu từ file Parquet (sử dụng basepath để đọc thư mục)
        df = spark.read.parquet(detail_path)
        
        # Ghi vào Iceberg thông qua Nessie Catalog
        print(f"Ghi bang {table_name} vao Iceberg...")
        # Sử dụng writeTo để tận dụng API V2 của Iceberg
        df.writeTo(table_name) \
          .tableProperty("write.format.default", "parquet") \
          .createOrReplace()
        
        print(f"Hoan thanh load du lieu {scale}!")
        
        # In ra số lượng record
        count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        print(f"-> Bang {table_name} co {count} dong.\n")
    
    spark.stop()

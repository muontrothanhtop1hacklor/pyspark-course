from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestS3A") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.jars", "/opt/spark-jars/hadoop-aws.jar,/opt/spark-jars/aws-java-sdk-bundle.jar") \
    .getOrCreate()

data = [("Alice", 1)]
df = spark.createDataFrame(data, ["Name", "Value"])
try:
    df.write.mode("overwrite").parquet("s3a://lakehouse/test_data2")
    print("SUCCESS")
except Exception as e:
    print(f"FAILED: {e}")
spark.stop()

import os
import sys

# Ép PySpark dùng trình biên dịch Python hiện tại của Conda
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

from pyspark.sql import Row

spark = SparkSession.builder.appName('clean_data').getOrCreate()

df = spark.createDataFrame([
    Row(name='Alice', height=80.1, age=5),
    Row(name='Bob', height=100.0, age=10),
    Row(name='BOB', height=float("nan"), age=5),
    Row(name='Tom', height=None, age=None),
    Row(name=None, height=float("nan"), age=None),
    Row(name='josh', height=78.9, age=9),
    Row(name='bush', height=1802.3, age=18),
    Row(name='jerry', height=75.3, age=7),
    ])
df2 = df.na.drop(subset="name")
df3 = df2.where(df2.height.between(65, 85))
df3.show()
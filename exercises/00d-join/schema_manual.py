from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Schema thu cong cho ca 2 bang, khong dung inferSchema.

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("customer_since", DateType(), nullable=True),
])

# orders.csv la file THAT cua bai 01-orders-aggregation, gom du 6 cot:
# order_id, customer_id, province, amount, status, order_date.
# Luu y: ORDER_SCHEMA trong spark_orders_exercise.py (bai 01) chi khai bao
# 5 field, THIEU order_date - Spark doc thieu cot ma khong bao loi. O day
# khai bao du 6 cot de khong lam mat du lieu order_date khi doc.
ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("province", StringType(), nullable=True),
    StructField("amount", DecimalType(12, 2), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("order_date", DateType(), nullable=True),
])

from pyspark.sql.types import StringType, StructField, StructType

# Schema "raw": khai bao thu cong, TAT CA la String.
# Ly do: tang doc (raw) chi nen giu nguyen du lieu goc; viec ep kieu se lam
# tuong minh o buoc sau (cast) de con nhin thay dong nao bi loi.
# 6 cot dau giong ORDER_SCHEMA cua bai 01-orders-aggregation (nhung kieu la String).
RAW_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("province", StringType(), nullable=True),
    StructField("amount", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),
    StructField("quantity", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("tags", StringType(), nullable=True),
    StructField("street", StringType(), nullable=True),
    StructField("district", StringType(), nullable=True),
    StructField("zip_code", StringType(), nullable=True),
])
"""
Unified PySpark Schemas for Orders and Customers
=================================================
Dinh nghia schema chung dung cho toan bo khoa hoc:
- Bronze/Raw layer: schema toan bo la StringType (giu nguyen du lieu goc).
- Silver/Validated layer: schema da ep dung kieu du lieu (Integer, Decimal, Date, Timestamp).
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# 1. Orders Schemas (Fact Table)
# ---------------------------------------------------------------------------

# Schema tho (Raw/Bronze) cho 6 cot co ban - tat ca la String
RAW_ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("province", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("order_date", StringType(), True),
])

# Schema chuan da cast (Silver/Gold) - 6 cot co ban
VALID_ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", StringType(), True),
    StructField("province", StringType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("status", StringType(), True),
    StructField("order_date", DateType(), True),
])

# Schema mo rong co updated_at phuc vu deduplication / SCD / Window (bai 06, 09)
EXTENDED_ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", StringType(), True),
    StructField("province", StringType(), True),
    StructField("amount", DecimalType(12, 2), True),
    StructField("status", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("updated_at", TimestampType(), True),
])

# ---------------------------------------------------------------------------
# 2. Customers Schemas (Dimension Table)
# ---------------------------------------------------------------------------

# Schema chuan cho bang khach hang
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("province", StringType(), True),
    StructField("customer_type", StringType(), True),
    StructField("created_at", TimestampType(), True),
])

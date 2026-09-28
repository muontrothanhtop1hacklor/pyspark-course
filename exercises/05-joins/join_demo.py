"""PySpark practice: inner join, left join va tach mapped/unmapped records.

Bai tap:
    + Tao orders va customers.
    + Thuc hanh inner join, left join.
    + Tao truong hop order khong tim thay customer.
    + Tach mapped/unmapped records.
    + So sanh ket qua inner join va left join.

orders.csv trong thu muc nay la file THAT cua bai 01-orders-aggregation,
khong sinh lai. Chi customers.csv la du lieu moi (xem generate_customers.py).
"""

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from schema_manual import CUSTOMERS_SCHEMA, ORDERS_SCHEMA

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

PROJECT_DIR = Path(__file__).resolve().parent
CUSTOMERS_PATH = PROJECT_DIR / "customers.csv"
# orders.csv la file THAT cua bai 01-orders-aggregation, tham chieu thang
# qua duong dan tuong doi - khong copy sang thu muc nay.
ORDERS_PATH = PROJECT_DIR.parent / "01-orders-aggregation" / "orders.csv"


def load_data(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    customers = (
        spark.read.option("header", True)
        .schema(CUSTOMERS_SCHEMA)
        .csv(str(CUSTOMERS_PATH))
    )
    orders = (
        spark.read.option("header", True)
        .schema(ORDERS_SCHEMA)
        .csv(str(ORDERS_PATH))
    )
    return customers, orders


def main() -> None:
    spark = (
        SparkSession.builder.appName("orders_customers_join")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        customers, orders = load_data(spark)

        print("=== Schema: customers ===")
        customers.printSchema()
        print("=== Schema: orders (file that cua bai 01) ===")
        orders.printSchema()

        print("=== customers ===")
        customers.show(truncate=False)
        print("=== orders ===")
        orders.show(truncate=False)

        # ---- Inner join: chi giu don co customer_id ton tai o ca 2 ben ----
        inner_df = orders.join(customers, on="customer_id", how="inner")
        print("=== INNER JOIN (orders x customers) ===")
        inner_df.orderBy("order_id").show(truncate=False)
        print(f"So dong INNER JOIN: {inner_df.count()}")

        # ---- Left join: giu TOAN BO orders, cot cua customers la NULL ----
        # neu khong tim thay customer_id tuong ung ----
        left_df = orders.join(customers, on="customer_id", how="left")
        print("=== LEFT JOIN (orders LEFT customers) ===")
        left_df.orderBy("order_id").show(truncate=False)
        print(f"So dong LEFT JOIN: {left_df.count()}")

        # ---- Tach mapped / unmapped tu ket qua left join ----
        # mapped: tim thay customer  |  unmapped: khong tim thay (customer_name la NULL)
        mapped_df = left_df.filter(F.col("customer_name").isNotNull())
        unmapped_df = left_df.filter(F.col("customer_name").isNull())

        print("=== Mapped records (co customer hop le) ===")
        mapped_df.select(
            "order_id", "customer_id", "province", "amount", "status", "order_date"
        ).orderBy("order_id").show(truncate=False)

        print("=== Unmapped records (KHONG tim thay customer - can kiem tra) ===")
        unmapped_df.select(
            "order_id", "customer_id", "province", "amount", "status", "order_date"
        ).orderBy("order_id").show(truncate=False)

        # ---- So sanh so dong: inner vs left vs unmapped ----
        print("=== So sanh so dong ===")
        print(f"orders goc       : {orders.count()}")
        print(f"inner join       : {inner_df.count()}")
        print(f"left join        : {left_df.count()}")
        print(f"mapped (trong left)   : {mapped_df.count()}")
        print(f"unmapped (trong left) : {unmapped_df.count()}")
        assert inner_df.count() == mapped_df.count(), (
            "So dong inner join phai bang so dong mapped trong left join"
        )
        assert left_df.count() == orders.count(), (
            "Left join khong duoc lam mat dong nao cua orders"
        )

        # ---- Doi chieu bang Spark SQL de thay ro chenh lech INNER vs LEFT ----
        orders.createOrReplaceTempView("orders")
        customers.createOrReplaceTempView("customers")
        print("=== SQL: cac order_id chi xuat hien trong LEFT JOIN, khong co trong INNER JOIN ===")
        spark.sql(
            """
            SELECT o.order_id, o.customer_id
            FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            ORDER BY o.order_id
            """
        ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

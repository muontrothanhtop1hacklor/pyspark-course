"""PySpark practice: read, transform, summarize, and write order data."""

from decimal import Decimal
import os
from pathlib import Path
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import count, sum as spark_sum
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

ORDER_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("province", StringType(), nullable=False),
        StructField("amount", DecimalType(12, 2), nullable=False),
        StructField("status", StringType(), nullable=False),
    ]
)


def summarize_successful_orders(orders: DataFrame) -> DataFrame:
    """Return order count and total amount for successful orders by province."""
    return (
        orders.filter(orders.status == "SUCCESS")
        .groupBy("province")
        .agg(
            count("order_id").alias("order_count"),
            spark_sum("amount").alias("total_amount"),
        )
        .orderBy("province")
    )


def main() -> None:
    project_dir = Path(__file__).resolve().parent
    input_path = project_dir / "orders.csv"
    output_path = project_dir / "output" / "orders_by_province"

    spark = (
        SparkSession.builder.appName("OrdersExercise").master("local[*]").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:        
        moc_oders = [
                (991, "C001", "ON", Decimal("100000.00"), "SUCCESS"),
                (992, "C002", "QC", Decimal("200000.00"), "FAILED"),
                (993, "C003", "BC", Decimal("150000.00"), "SUCCESS"),
                (994, "C004", "ON", Decimal("300000.00"), "SUCCESS"),
                (995, "C005", "QC", Decimal("250000.00"), "FAILED"),
                (996, "C006", "BC", Decimal("400000.00"), "SUCCESS"),
                (997, "C007", "ON", Decimal("500000.00"), "FAILED"),
                (998, "C008", "QC", Decimal("350000.00"), "SUCCESS"),
                (999, "C009", "BC", Decimal("600000.00"), "FAILED"),
                (1000, "C010", "ON", Decimal("700000.00"), "SUCCESS"),
                ]        

        df_mock = spark.createDataFrame(moc_oders, schema=ORDER_SCHEMA)
        df_csv = (
            spark.read.option("header", True)
            .schema(ORDER_SCHEMA)
            .csv(str(input_path))
        )

        orders = df_mock.unionByName(df_csv)

        print("=== Schema ===")
        orders.printSchema()

        print("=== All orders ===")
        orders.show(truncate=False)

        print("=== Selected columns ===")
        orders.select("order_id", "province", "amount").show()

        successful_orders = orders.filter(orders.status == "SUCCESS")
        print("=== Successful orders ===")
        successful_orders.show(truncate=False)

        province_summary = summarize_successful_orders(orders)
        print("=== Successful orders by province ===")
        province_summary.show()

        orders.createOrReplaceTempView("orders")
        print("=== SQL: total amount by province (SUCCESS statuses) ===")
        spark.sql(
            """
            SELECT province, SUM(amount) AS total_amount
            FROM orders
            WHERE status = 'SUCCESS'
            GROUP BY province
            ORDER BY province
            """
        ).show()

        province_summary.write.mode("overwrite").option("header", True).csv(
            str(output_path)
        )
        print(f"=== Wrote output to {output_path} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""PySpark practice: khai bao schema thu cong, cast kieu du lieu va quan sat loi cast."""

import logging
import os
import sys
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from schema_manual import RAW_ORDERS_SCHEMA

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

CSV_PATH = Path(__file__).resolve().parent / "raw_orders_types.csv"


def build_scalar_casts() -> dict:
    """Cac cot scalar can cast: ten cot -> bieu thuc cast.

    Dung chung cho ca buoc cast lan buoc kiem tra loi, tranh viet lap.
    Phai goi SAU khi SparkSession da duoc tao (F.col can SparkContext dang chay).
    """
    return {
        "order_id": F.col("order_id").cast("int"),
        "amount": F.col("amount").cast("decimal(12,2)"),
        "quantity": F.col("quantity").cast("int"),
        "order_date": F.to_date("order_date", "yyyy-MM-dd"),
        "created_at": F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"),
    }


def demo_double_vs_decimal(spark: SparkSession) -> None:
    """Vi sao tien te nen dung Decimal thay vi Double/Float."""
    spark.sql(
        """
        SELECT
            CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE)                AS double_sum,
            CAST(0.1 AS FLOAT)  + CAST(0.2 AS FLOAT)                 AS float_sum,
            CAST(0.1 AS DECIMAL(12,2)) + CAST(0.2 AS DECIMAL(12,2))  AS decimal_sum
        """
    ).show(truncate=False)


def demo_ansi_mode(spark: SparkSession, raw_df: DataFrame) -> None:
    """Cung mot phep cast 'abc' -> so: ANSI ON nem exception, try_cast tra NULL."""
    spark.conf.set("spark.sql.ansi.enabled", "true")
    # Tam tat log de stack trace cua Spark khong tran console khi co exception.
    spark.sparkContext.setLogLevel("OFF")
    ctx_logger = logging.getLogger("DataFrameQueryContextLogger")
    old_level = ctx_logger.level
    ctx_logger.setLevel(logging.CRITICAL)
    try:
        raw_df.select(F.col("amount").cast("decimal(12,2)")).collect()
        print("ANSI=true: khong co loi (bat thuong)")
    except Exception as exc:  # noqa: BLE001 - chi de in ra hanh vi cho bai hoc
        first_line = str(exc).strip().splitlines()[0]
        print(f"ANSI=true  -> nem loi: {type(exc).__name__}: {first_line}")
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "false")
        spark.sparkContext.setLogLevel("WARN")
        ctx_logger.setLevel(old_level)

    print("ANSI=false -> try_cast tra NULL, khong lam sap job:")
    raw_df.select(
        "order_id",
        "amount",
        F.expr("try_cast(amount AS decimal(12,2))").alias("amount_try_cast"),
    ).filter(F.col("order_id") == "2007").show(truncate=False)


def main() -> None:
    spark = (
        SparkSession.builder.appName("chapter2_data_types")
        .master("local[*]")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = (
            spark.read.option("header", True)
            .schema(RAW_ORDERS_SCHEMA)
            .csv(str(CSV_PATH))
        )

        print("=== Schema truoc khi cast (tat ca la String) ===")
        raw_df.printSchema()

        # ---- Cast cac cot scalar ----
        scalar_casts = build_scalar_casts()
        casted_df = raw_df
        for col_name, expr in scalar_casts.items():
            casted_df = casted_df.withColumn(col_name, expr)

        # ---- Cast cot phuc tap: Array + Struct ----
        casted_df = (
            casted_df.withColumn("tags", F.split(F.col("tags"), ","))
            .withColumn(
                "shipping_address",
                F.struct(
                    F.col("street").alias("street"),
                    F.col("district").alias("district"),
                    F.col("zip_code").alias("zip_code"),
                ),
            )
            .drop("street", "district", "zip_code")
        )

        print("=== Schema sau khi cast ===")
        casted_df.printSchema()

        print("=== Toan bo du lieu sau cast ===")
        casted_df.show(truncate=False)

        # ---- Bao cao loi ----
        # NULL sau cast co 2 nguyen nhan khac nhau:
        #   (1) gia tri goc da la NULL/rong  -> khong phai loi cast
        #   (2) gia tri goc co, nhung cast that bai -> loi dinh dang that su
        # Chi so sanh raw vs casted moi phan biet duoc hai truong hop nay.
        print("=== So luong null theo tung cot sau cast (gom ca (1) va (2)) ===")
        casted_df.select(
            [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in scalar_casts]
        ).show()

        failed_flags = {
            c: F.col(c).isNotNull() & expr.isNull() for c, expr in scalar_casts.items()
        }

        print("=== So luong cast loi THAT SU (gia tri goc co, cast ra NULL) ===")
        raw_df.select(
            [F.sum(flag.cast("int")).alias(c) for c, flag in failed_flags.items()]
        ).show()

        print("=== Cac dong cast loi (hien thi GIA TRI GOC de thay nguyen nhan) ===")
        any_failed = reduce(lambda a, b: a | b, failed_flags.values())
        raw_df.filter(any_failed).select(
            "order_id", "amount", "quantity", "order_date", "created_at"
        ).show(truncate=False)

        print("=== Cac dong NULL tu nguon (khong phai loi cast) ===")
        raw_df.filter(F.col("created_at").isNull()).select(
            "order_id", "created_at"
        ).show(truncate=False)

        print("=== Cast THANH CONG nhung sai nghiep vu (amount < 0) ===")
        casted_df.filter(F.col("amount") < 0).select("order_id", "amount").show(
            truncate=False
        )

        print("=== Double / Float / Decimal ===")
        demo_double_vs_decimal(spark)

        print("=== ANSI mode: cast loi -> NULL hay nem exception ===")
        demo_ansi_mode(spark, raw_df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
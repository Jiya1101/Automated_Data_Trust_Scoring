"""
multi_source_loader.py
----------------------
Loads all REAL CSV datasets from DataTrustFramework/data/raw/multi_source/
and the main transactions.csv, then normalises each one into the unified schema:

    user_id | amount | city | timestamp | source_id

NO synthetic data, NO random generation - only reads existing CSV files.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, to_timestamp, coalesce,
    monotonically_increasing_id, current_timestamp, cast
)
from pyspark.sql.types import DoubleType, StringType


# ---------------------------------------------------------------------------
# Helper: ensure a DataFrame has exactly the target columns in correct types
# ---------------------------------------------------------------------------
TARGET_COLS = ["user_id", "amount", "city", "timestamp", "source_id"]


def _select_unified(df: DataFrame) -> DataFrame:
    """Select and cast to the unified schema - returns only TARGET_COLS."""
    return df.select(
        col("user_id").cast(StringType()).alias("user_id"),
        col("amount").cast(DoubleType()).alias("amount"),
        col("city").cast(StringType()).alias("city"),
        col("timestamp").cast(StringType()).alias("timestamp"),
        col("source_id").cast(StringType()).alias("source_id"),
    )


# ---------------------------------------------------------------------------
# 1. Transactions dataset  (user_id, purchase_amount, city, timestamp, source_id)
# ---------------------------------------------------------------------------
def load_transactions(spark: SparkSession, base_path: str) -> DataFrame:
    """
    Loads data/raw/transactions.csv (the original synthetic-free pre-existing file).
    Columns: user_id, purchase_amount, city, timestamp, source_id
    """
    path = os.path.join(base_path, "transactions.csv")
    print(f"[LOAD] transactions  <- {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)

    df = (
        df
        # purchase_amount  -> amount
        .withColumnRenamed("purchase_amount", "amount")
        # source_id already present; overwrite for consistency
        .withColumn("source_id", lit("transactions"))
    )

    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    return _select_unified(df)


# ---------------------------------------------------------------------------
# 2. E-commerce dataset  (order_id, customer_name, order_value, location, order_date)
# ---------------------------------------------------------------------------
def load_ecommerce(spark: SparkSession, base_path: str) -> DataFrame:
    """
    Loads data/raw/multi_source/ecommerce_data.csv
    Columns: order_id, customer_name, order_value, location, order_date
    """
    path = os.path.join(base_path, "ecommerce_data.csv")
    print(f"[LOAD] ecommerce     <- {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)

    df = (
        df
        # order_id    -> user_id  (closest proxy for a unique record identifier)
        .withColumnRenamed("order_id", "user_id")
        # order_value -> amount
        .withColumnRenamed("order_value", "amount")
        # location    -> city
        .withColumnRenamed("location", "city")
        # order_date  -> timestamp
        .withColumnRenamed("order_date", "timestamp")
        # tag source
        .withColumn("source_id", lit("ecommerce"))
    )

    df = df.withColumn("ingestion_timestamp", current_timestamp())

    return _select_unified(df)


# ---------------------------------------------------------------------------
# 3. Subscription dataset  (subscription_id, user_uuid, monthly_billing, city_name, created_at)
# ---------------------------------------------------------------------------
def load_subscription(spark: SparkSession, base_path: str) -> DataFrame:
    """
    Loads data/raw/multi_source/subscription_data.csv
    Columns: subscription_id, user_uuid, monthly_billing, city_name, created_at
    """
    path = os.path.join(base_path, "subscription_data.csv")
    print(f"[LOAD] subscription  <- {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)

    df = (
        df
        # user_uuid        -> user_id  (unique user identifier)
        .withColumnRenamed("user_uuid", "user_id")
        # monthly_billing  -> amount
        .withColumnRenamed("monthly_billing", "amount")
        # city_name        -> city
        .withColumnRenamed("city_name", "city")
        # created_at       -> timestamp
        .withColumnRenamed("created_at", "timestamp")
        .withColumn("source_id", lit("subscription"))
    )

    df = df.withColumn("ingestion_timestamp", current_timestamp())

    return _select_unified(df)


# ---------------------------------------------------------------------------
# 4. Taxi dataset  (trip_id, passenger_id, fare_amount, pickup_location, trip_timestamp)
# ---------------------------------------------------------------------------
def load_taxi(spark: SparkSession, base_path: str) -> DataFrame:
    """
    Loads data/raw/multi_source/taxi_data.csv
    Columns: trip_id, passenger_id, fare_amount, pickup_location, trip_timestamp
    """
    path = os.path.join(base_path, "taxi_data.csv")
    print(f"[LOAD] taxi          <- {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)

    df = (
        df
        # passenger_id     -> user_id
        .withColumnRenamed("passenger_id", "user_id")
        # fare_amount      -> amount
        .withColumnRenamed("fare_amount", "amount")
        # pickup_location  -> city
        .withColumnRenamed("pickup_location", "city")
        # trip_timestamp   -> timestamp
        .withColumnRenamed("trip_timestamp", "timestamp")
        .withColumn("source_id", lit("taxi"))
    )

    df = df.withColumn("ingestion_timestamp", current_timestamp())

    return _select_unified(df)


# ---------------------------------------------------------------------------
# Public API: load_all_sources
# ---------------------------------------------------------------------------
def load_all_sources(spark: SparkSession) -> DataFrame:
    """
    Loads ALL real CSV datasets, normalises them into the unified schema, and
    returns a single combined DataFrame:

        user_id | amount | city | timestamp | source_id

    Data origins:
        - data/raw/transactions.csv
        - data/raw/multi_source/ecommerce_data.csv
        - data/raw/multi_source/subscription_data.csv
        - data/raw/multi_source/taxi_data.csv
    """

    raw_root = "DataTrustFramework/data/raw"
    multi_source_path = os.path.join(raw_root, "multi_source")

    # Load each source individually
    df_transactions = load_transactions(spark, raw_root)
    df_ecommerce    = load_ecommerce(spark, multi_source_path)
    df_subscription = load_subscription(spark, multi_source_path)
    df_taxi         = load_taxi(spark, multi_source_path)

    print("\n[UNION] Combining all sources via unionByName ...")

    # Union all into one DataFrame
    df_combined = (
        df_transactions
        .unionByName(df_ecommerce)
        .unionByName(df_subscription)
        .unionByName(df_taxi)
    )

    # -----------------------------------------------------------------------
    # Post-union cleanup
    # -----------------------------------------------------------------------

    # 1. Drop rows where amount is null (unprocessable records)
    df_combined = df_combined.filter(col("amount").isNotNull())

    # 2. Drop rows where amount <= 0 (sanity guard; negative fares are invalid)
    df_combined = df_combined.filter(col("amount") > 0)

    # 3. Fill missing city with "unknown"
    df_combined = df_combined.fillna({"city": "unknown"})

    # 4. Drop rows with null timestamp (pipeline needs it for freshness/timeliness)
    df_combined = df_combined.filter(col("timestamp").isNotNull())

    # 5. Add ingestion_timestamp for the downstream pipeline modules
    df_combined = df_combined.withColumn("ingestion_timestamp", current_timestamp())

    print(f"\n[INFO] Schema after combining:")
    df_combined.printSchema()

    return df_combined


# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------
def validate_and_show(df: DataFrame) -> None:
    """Print schema, sample rows, and record count for sanity checking."""
    print("\n" + "=" * 60)
    print("MULTI-SOURCE DATASET VALIDATION")
    print("=" * 60)

    df.printSchema()

    print("\n[SAMPLE] 10 rows across all sources:")
    df.show(10, truncate=False)

    total = df.count()
    print(f"\n[COUNT] Total records: {total:,}")

    print("\n[BREAKDOWN] Records per source_id:")
    df.groupBy("source_id").count().orderBy("source_id").show()
    print("=" * 60 + "\n")

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnull, current_timestamp, datediff, lit
from pyspark.sql.types import DoubleType

def compute_quality_metrics(df: DataFrame) -> DataFrame:
    """
    Computes data quality metrics for each record:
    - Completeness
    - Consistency
    - Accuracy
    - Freshness
    - Timeliness
    Returns a dataframe with these new metric columns normalized between 0 and 1.

    Works with the UNIFIED schema:  user_id | amount | city | timestamp | source_id
    """

    # 1. Dynamic Completeness: Ratio of non-null fields across ALL valid input columns
    # This prevents arbitrary high-quality ML sets (like creditcard.csv) from tanking simply because they don't have a 'city' column.
    schema_cols = [c for c in df.columns if c not in ["ingestion_timestamp", "completeness", "consistency", "accuracy", "freshness", "timeliness", "is_anomaly", "anomaly_severity"]]
    total_fields = max(1.0, float(len(schema_cols)))
    
    completeness_expr = lit(0.0)
    for c in schema_cols:
        completeness_expr = completeness_expr + when(isnull(col(c)), 0.0).otherwise(1.0)
    completeness_expr = completeness_expr / lit(total_fields)
    
    df = df.withColumn("completeness", completeness_expr.cast(DoubleType()))

    # 2. Consistency: 1.0 if amount is present and non-null
    consistency_expr = when(
        isnull(col("amount")), 0.0
    ).otherwise(1.0)
    df = df.withColumn("consistency", consistency_expr.cast(DoubleType()))

    # 3. Accuracy: amount within an acceptable range (0 – 100,000).
    #    Covers all source types: taxi fares, subscription fees, e-commerce orders,
    #    and general transactions.
    accuracy_expr = when(
        isnull(col("amount")), 0.0
    ).when(
        (col("amount") >= 0) & (col("amount") <= 100_000), 1.0
    ).otherwise(0.0)
    df = df.withColumn("accuracy", accuracy_expr.cast(DoubleType()))

    # 4. Freshness: How recent is the record timestamp?
    #    Uses 365 days as max decay window (multi-source data can be months old).
    days_old_expr = datediff(current_timestamp(), col("timestamp"))
    freshness_expr = when(
        isnull(col("timestamp")), 0.0
    ).when(
        days_old_expr <= 0, 1.0
    ).when(
        days_old_expr >= 365, 0.0
    ).otherwise(
        1.0 - (days_old_expr / 365.0)
    )
    df = df.withColumn("freshness", freshness_expr.cast(DoubleType()))

    # 5. Timeliness: Difference between ingestion_timestamp and record timestamp.
    #    Records arriving within 90 days are fully timely; > 365 days = 0.
    arrival_delay_expr = datediff(col("ingestion_timestamp"), col("timestamp"))
    timeliness_expr = when(
        isnull(col("timestamp")), 0.0
    ).when(
        arrival_delay_expr <= 7, 1.0
    ).when(
        arrival_delay_expr >= 365, 0.0
    ).otherwise(
        1.0 - ((arrival_delay_expr - 7) / 358.0)
    )
    df = df.withColumn("timeliness", timeliness_expr.cast(DoubleType()))

    return df

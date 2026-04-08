"""
Streaming Quality Metrics module for Data Trust Scoring Framework.
Adapted to work with streaming DataFrames (no stateful operations beyond grouping).
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnull, current_timestamp, datediff, lit
from pyspark.sql.types import DoubleType


def compute_quality_metrics_streaming(df: DataFrame) -> DataFrame:
    """
    Computes data quality metrics for streaming records:
    - Completeness
    - Consistency
    - Accuracy
    - Freshness
    - Timeliness
    
    Returns a DataFrame with these new metric columns normalized between 0 and 1.
    Compatible with both batch and streaming DataFrames.
    
    Args:
        df: Streaming DataFrame
        
    Returns:
        Transformed DataFrame with quality metrics
    """
    
    # Define columns to check for completeness
    total_fields = 5.0  # user_id, purchase_amount, city, timestamp, source_id
    
    # 1. Completeness: Ratio of non-null fields
    completeness_expr = (
        when(isnull(col("user_id")), 0.0).otherwise(1.0) +
        when(isnull(col("purchase_amount")), 0.0).otherwise(1.0) +
        when(isnull(col("city")), 0.0).otherwise(1.0) +
        when(isnull(col("timestamp")), 0.0).otherwise(1.0) +
        when(isnull(col("source_id")), 0.0).otherwise(1.0)
    ) / lit(total_fields)
    
    df = df.withColumn("completeness", completeness_expr.cast(DoubleType()))
    
    # 2. Consistency: Fields should have valid types/structure
    consistency_expr = when(
        isnull(col("purchase_amount")), 0.0
    ).otherwise(1.0)
    df = df.withColumn("consistency", consistency_expr.cast(DoubleType()))
    
    # 3. Accuracy: Numeric fields within acceptable range
    # Acceptable purchase amount: 0 to 10000
    accuracy_expr = when(
        isnull(col("purchase_amount")), 0.0
    ).when(
        (col("purchase_amount") >= 0) & (col("purchase_amount") <= 10000), 1.0
    ).otherwise(0.0)
    df = df.withColumn("accuracy", accuracy_expr.cast(DoubleType()))
    
    # 4. Freshness: How recent is the record timestamp?
    # Using 30 days as max freshness decay window
    # 1.0 is fresh (today), 0.0 is > 30 days old
    days_old_expr = datediff(current_timestamp(), col("timestamp"))
    freshness_expr = when(
        isnull(col("timestamp")), 0.0
    ).when(
        days_old_expr <= 0, 1.0
    ).when(
        days_old_expr >= 30, 0.0
    ).otherwise(
        1.0 - (days_old_expr / 30.0)
    )
    df = df.withColumn("freshness", freshness_expr.cast(DoubleType()))
    
    # 5. Timeliness: Did record arrive within acceptable time window?
    # If difference between record timestamp and ingestion > 45 days, it's late (0.0)
    arrival_delay_expr = datediff(col("ingestion_timestamp"), col("timestamp"))
    timeliness_expr = when(
        isnull(col("timestamp")), 0.0
    ).when(
        arrival_delay_expr <= 7, 1.0
    ).when(
        arrival_delay_expr >= 45, 0.0
    ).otherwise(
        1.0 - ((arrival_delay_expr - 7) / 38.0)
    )
    df = df.withColumn("timeliness", timeliness_expr.cast(DoubleType()))
    
    return df

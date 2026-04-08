"""
Streaming Source Reputation module for Data Trust Scoring Framework.
Uses windowed aggregations to calculate rolling source reputation scores.

In streaming, we can't access global statistics. Instead, we use:
- Window functions to calculate reputation over recent time windows
- Exponential moving average for smooth reputation tracking
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, sum, count, avg, row_number, lit, when,
    from_unixtime, unix_timestamp, expr
)
import logging

logger = logging.getLogger(__name__)


def compute_source_reputation_streaming(df: DataFrame, 
                                       window_duration: str = "1 hour",
                                       sliding_duration: str = "10 minutes") -> DataFrame:
    """
    Calculate source reputation using sliding window aggregation.
    
    Formula: Rep(source) = 1 - (anomalies_in_window / total_in_window)
    
    This is suitable for streaming where we calculate metrics over recent time windows
    rather than the entire dataset.
    
    Args:
        df: Streaming DataFrame (must have 'is_anomaly', 'source_id', 'timestamp')
        window_duration: Window size (e.g., "1 hour", "30 minutes")
        sliding_duration: Sliding interval (e.g., "10 minutes")
        
    Returns:
        DataFrame with source_reputation column added
    """
    
    logger.info(f"Computing streaming source reputation with window: {window_duration}, slide: {sliding_duration}")
    
    # Define window specification for aggregation
    window_spec = Window \
        .partitionBy("source_id") \
        .orderBy(col("timestamp").cast("long")) \
        .rangeBetween(
            -int(window_duration.split()[0]) * 3600,  # Rough conversion for 1 hour = 3600 seconds
            0
        )
    
    # Calculate anomaly count and total count within the window using Window functions
    df = df.withColumn(
        "anomalies_in_window",
        sum(col("is_anomaly").cast("int")).over(window_spec)
    )
    
    df = df.withColumn(
        "total_in_window",
        count("*").over(window_spec)
    )
    
    # Calculate reputation
    # Avoid division by zero
    df = df.withColumn(
        "source_reputation",
        when(col("total_in_window") > 0,
             1.0 - (col("anomalies_in_window") / col("total_in_window"))
        ).otherwise(0.5)  # Default to neutral if no records in window
    )
    
    # Drop intermediate columns if desired, or keep them for debugging
    # df = df.drop("anomalies_in_window", "total_in_window")
    
    return df


def compute_source_reputation_exponential_moving_average(
    df: DataFrame,
    alpha: float = 0.3
) -> DataFrame:
    """
    Calculate source reputation using Exponential Moving Average.
    Better for streaming as it gives more weight to recent records.
    
    Formula: Rep_new = alpha * current_anomaly_rate + (1 - alpha) * Rep_old
    
    Note: This requires stateful streaming (not pure Spark SQL).
    For pure Spark SQL streaming, use compute_source_reputation_streaming instead.
    
    Args:
        df: Streaming DataFrame
        alpha: Smoothing factor (0-1). Higher = more weight on recent data.
        
    Returns:
        DataFrame with exponential moving average reputation
    """
    
    logger.info(f"Computing EMA-based reputation with alpha={alpha}")
    
    # For now, use a simplified approach with recent window
    # In production, use StateStore in mapGroupsWithState for true stateful streaming
    
    # Use past 10 minutes as "recent" window
    window_spec = Window \
        .partitionBy("source_id") \
        .orderBy(col("timestamp").cast("long")) \
        .rangeBetween(-600, 0)  # 600 seconds = 10 minutes
    
    df = df.withColumn(
        "recent_anomaly_rate",
        when(count("*").over(window_spec) > 0,
             sum(col("is_anomaly").cast("int")).over(window_spec) / count("*").over(window_spec)
        ).otherwise(0.0)
    )
    
    # EMA reputation (simplified)
    # In production, track previous reputation in state store
    df = df.withColumn(
        "source_reputation",
        1.0 - col("recent_anomaly_rate")
    )
    
    return df


def compute_source_reputation_batch_reference(df: DataFrame) -> DataFrame:
    """
    For comparison: Calculate reputation on entire batch/window.
    Good for initial training or periodic recomputation.
    
    This is similar to the batch version but still works on streaming dataframes.
    
    Args:
        df: DataFrame
        
    Returns:
        DataFrame with source_reputation
    """
    
    logger.info("Computing batch reference source reputation...")
    
    # Global aggregation (works on each micro-batch)
    agg_df = df.groupBy("source_id").agg(
        sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        count("*").alias("total_records")
    )
    
    agg_df = agg_df.withColumn(
        "source_reputation",
        when(col("total_records") > 0,
             1.0 - (col("anomaly_count") / col("total_records"))
        ).otherwise(0.5)
    )
    
    reputation_lookup = agg_df.select("source_id", "source_reputation")
    
    # Join back to main DataFrame
    from pyspark.sql.functions import broadcast
    joined_df = df.join(
        broadcast(reputation_lookup),
        on="source_id",
        how="left"
    )
    
    return joined_df

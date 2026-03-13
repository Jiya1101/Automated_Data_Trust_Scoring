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
    """
    
    # Define columns to check for completeness (all original columns except ingestion_timestamp)
    # user_id, purchase_amount, city, timestamp, source_id
    total_fields = 5.0
    
    # 1. Completeness: Ratio of non-null fields
    # Example logic: count non-nulls / total_fields
    completeness_expr = (
        when(isnull(col("user_id")), 0.0).otherwise(1.0) +
        when(isnull(col("purchase_amount")), 0.0).otherwise(1.0) +
        when(isnull(col("city")), 0.0).otherwise(1.0) +
        when(isnull(col("timestamp")), 0.0).otherwise(1.0) +
        when(isnull(col("source_id")), 0.0).otherwise(1.0)
    ) / lit(total_fields)
    
    df = df.withColumn("completeness", completeness_expr.cast(DoubleType()))
    
    # 2. Consistency: Basic schema validation checks (e.g., negative amounts shouldn't happen)
    # Wait, the prompt says "schema validation and correct data types". Since PySpark enforces schema, 
    # we can check logic consistency. Let's define consistency as 1.0 if purchase_amount is numeric and non-empty.
    # We will give 1.0 if everything matches what we expect syntactically.
    consistency_expr = when(
        isnull(col("purchase_amount")), 0.0
    ).otherwise(1.0)
    df = df.withColumn("consistency", consistency_expr.cast(DoubleType()))
    
    # 3. Accuracy: Numeric fields within acceptable range. 
    # Let's say acceptable purchase amount is between 0 and 10000.
    accuracy_expr = when(
        isnull(col("purchase_amount")), 0.0
    ).when(
        (col("purchase_amount") >= 0) & (col("purchase_amount") <= 10000), 1.0
    ).otherwise(0.0)
    df = df.withColumn("accuracy", accuracy_expr.cast(DoubleType()))
    
    # 4. Freshness: Difference between current time and record timestamp.
    # Using 30 days as max freshness decay window. 1.0 is fresh (today), 0.0 is > 30 days old.
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
    
    # 5. Timeliness: Checks if record arrived within acceptable time window.
    # Let's say if the difference between record timestamp and ingestion_timestamp is > 45 days, it's late (0.0).
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

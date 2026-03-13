from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, count

def compute_source_reputation(df: DataFrame) -> DataFrame:
    """
    Calculates reputation for each source using formula:
    Rep(source) = 1 - (anomaly_count / total_records)
    Adds reputation score to each record.
    """
    
    # 1. Aggregate anomalies and total records per source
    agg_df = df.groupBy("source_id").agg(
        sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
        count("*").alias("total_records")
    )
    
    # 2. Compute the reputation score
    # Rep(source) = 1 - (anomaly_count / total_records)
    agg_df = agg_df.withColumn(
        "source_reputation",
        1.0 - (col("anomaly_count") / col("total_records"))
    )
    
    # Select only what we need to join back
    reputation_lookup = agg_df.select("source_id", "source_reputation")
    
    # 3. Join the reputation back to the main DataFrame
    # Using a broadcast join could be efficient here if the number of sources is small.
    # Since we have only 3 sources, broadcast join is perfect.
    from pyspark.sql.functions import broadcast
    
    joined_df = df.join(broadcast(reputation_lookup), on="source_id", how="left")
    
    return joined_df

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, datediff, current_timestamp, exp, lit

def compute_trust_score(df: DataFrame) -> DataFrame:
    """
    Applies exponential decay based on ingestion date.
    Computes final trust score.
    TRUST_SCORE = ((Completeness + Consistency + Accuracy + Freshness + Timeliness) / 5) x Source_Reputation x Temporal_Decay
    """
    
    # 1. Temporal Decay
    # Decay(t) = exp(-0.05 * days_since_ingestion)
    # days_since_ingestion is datediff between current_timestamp() and ingestion_timestamp
    days_since_ingest = datediff(current_timestamp(), col("ingestion_timestamp"))
    
    # If the record was just ingested, days_since_ingest will be 0, decay will be exp(0) = 1.0
    decay_expr = exp(lit(-0.05) * days_since_ingest)
    
    df = df.withColumn("temporal_decay", decay_expr)
    
    # 2. Final Trust Score Calculation
    dimensions_avg = (
        col("completeness") + 
        col("consistency") + 
        col("accuracy") + 
        col("freshness") + 
        col("timeliness")
    ) / 5.0
    
    trust_score_expr = dimensions_avg * col("source_reputation") * col("temporal_decay")
    
    # Output must be normalized 0-100 since the above produces values 0.0 to 1.0.
    normalized_trust_score = trust_score_expr * 100.0
    
    df = df.withColumn("trust_score", normalized_trust_score)
    
    return df

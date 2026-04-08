"""
Streaming Trust Score module for Data Trust Scoring Framework.
Computes final trust scores for streaming records.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, datediff, current_timestamp, exp, lit
import logging

logger = logging.getLogger(__name__)


def compute_trust_score_streaming(df: DataFrame) -> DataFrame:
    """
    Applies exponential temporal decay and computes final trust score.
    
    Formula:
    TRUST_SCORE = ((Completeness + Consistency + Accuracy + Freshness + Timeliness) / 5) 
                  × Source_Reputation × Temporal_Decay
    
    Result is normalized to 0-100 scale.
    
    Args:
        df: Streaming DataFrame with quality metrics, anomaly status, and reputation
        
    Returns:
        DataFrame with trust_score column added
    """
    
    logger.info("Computing trust scores...")
    
    # 1. Temporal Decay
    # Decay(t) = exp(-0.05 * days_since_ingestion)
    days_since_ingest = datediff(current_timestamp(), col("ingestion_timestamp"))
    
    decay_expr = exp(lit(-0.05) * days_since_ingest.cast("double"))
    
    df = df.withColumn("temporal_decay", decay_expr)
    
    # 2. Quality Dimensions Average
    # Average of: completeness, consistency, accuracy, freshness, timeliness
    dimensions_avg = (
        col("completeness") + 
        col("consistency") + 
        col("accuracy") + 
        col("freshness") + 
        col("timeliness")
    ) / 5.0
    
    # 3. Final Trust Score
    # Combine all factors: quality * reputation * temporal_decay
    # Handle null values gracefully
    from pyspark.sql.functions import coalesce
    
    trust_score_expr = (
        dimensions_avg * 
        coalesce(col("source_reputation"), lit(0.5)) * 
        decay_expr
    )
    
    # Normalize to 0-100 scale
    normalized_trust_score = trust_score_expr * 100.0
    
    df = df.withColumn("trust_score", normalized_trust_score)
    
    logger.info("Trust scores computed successfully")
    
    return df


def prepare_output_columns(df: DataFrame, include_intermediate: bool = False) -> DataFrame:
    """
    Select and order output columns for final results.
    
    Args:
        df: Processed DataFrame
        include_intermediate: Whether to include intermediate metric columns
        
    Returns:
        DataFrame with selected columns
    """
    
    if include_intermediate:
        # Include all metrics for detailed analysis
        output_cols = [
            "user_id", "source_id", "purchase_amount", "city", "timestamp",
            "completeness", "consistency", "accuracy", "freshness", "timeliness",
            "is_anomaly", "anomaly_severity",
            "source_reputation", "temporal_decay", "trust_score"
        ]
    else:
        # Only include essential columns
        output_cols = [
            "user_id", "source_id", "timestamp",
            "is_anomaly", "source_reputation", "trust_score"
        ]
    
    # Filter to only columns that exist
    existing_cols = [col for col in output_cols if col in df.columns]
    
    return df.select(existing_cols)

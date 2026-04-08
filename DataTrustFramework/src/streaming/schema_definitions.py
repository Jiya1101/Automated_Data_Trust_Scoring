"""
Schema definitions for streaming data.
Defines the structure of Kafka messages and intermediate streaming DataFrames.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, BooleanType, LongType
)

# ============================================================
# Input Schema: JSON messages from Kafka
# ============================================================
KAFKA_INPUT_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("purchase_amount", DoubleType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),  # ISO format string
    StructField("source_id", StringType(), nullable=False),
])

# ============================================================
# Streaming DataFrame Schema after parsing and initial processing
# ============================================================
STREAM_PROCESSING_SCHEMA = StructType([
    # Original fields
    StructField("user_id", StringType(), nullable=False),
    StructField("purchase_amount", DoubleType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("source_id", StringType(), nullable=False),
    
    # Added by ingestion
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    
    # Quality metrics (added by quality_metrics module)
    StructField("completeness", DoubleType(), nullable=True),
    StructField("consistency", DoubleType(), nullable=True),
    StructField("accuracy", DoubleType(), nullable=True),
    StructField("freshness", DoubleType(), nullable=True),
    StructField("timeliness", DoubleType(), nullable=True),
    
    # Anomaly detection (added by isolation_forest module)
    StructField("is_anomaly", BooleanType(), nullable=True),
    StructField("anomaly_severity", DoubleType(), nullable=True),
    
    # Source reputation (added by source_reputation module)
    StructField("source_reputation", DoubleType(), nullable=True),
    
    # Temporal decay (added by trust_score module)
    StructField("temporal_decay", DoubleType(), nullable=True),
    
    # Final output
    StructField("trust_score", DoubleType(), nullable=True),
])

# ============================================================
# Output Schema: Final trust scores
# ============================================================
OUTPUT_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("source_id", StringType(), nullable=False),
    StructField("purchase_amount", DoubleType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("completeness", DoubleType(), nullable=True),
    StructField("consistency", DoubleType(), nullable=True),
    StructField("accuracy", DoubleType(), nullable=True),
    StructField("freshness", DoubleType(), nullable=True),
    StructField("timeliness", DoubleType(), nullable=True),
    StructField("is_anomaly", BooleanType(), nullable=True),
    StructField("anomaly_severity", DoubleType(), nullable=True),
    StructField("source_reputation", DoubleType(), nullable=True),
    StructField("temporal_decay", DoubleType(), nullable=True),
    StructField("trust_score", DoubleType(), nullable=True),
])

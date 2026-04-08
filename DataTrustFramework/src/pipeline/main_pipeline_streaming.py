"""
Main Streaming Pipeline for Data Trust Scoring Framework.
Real-time processing using Apache Kafka and PySpark Structured Streaming.

Architecture:
  Kafka Producer → Kafka Topic → Spark Structured Streaming → Pipeline → Output Sink

Usage:
  python src/pipeline/main_pipeline_streaming.py
"""

import os
import sys
import time
import logging
from typing import Optional

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.pipeline.spark_session import create_spark_session
from src.streaming.schema_definitions import KAFKA_INPUT_SCHEMA
from src.streaming.load_kafka_stream import load_and_parse_kafka
from src.streaming.quality_metrics_streaming import compute_quality_metrics_streaming
from src.streaming.anomaly_detection_streaming import detect_anomalies_streaming, AnomalyDetectionModel
from src.streaming.source_reputation_streaming import compute_source_reputation_streaming
from src.streaming.trust_score_streaming import compute_trust_score_streaming, prepare_output_columns

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamingPipelineConfig:
    """Configuration for streaming pipeline."""
    
    def __init__(self):
        self.kafka_bootstrap_servers = "localhost:9092"
        self.kafka_topic = "transactions"
        self.starting_offset = "latest"  # or "earliest"
        
        # Output options
        self.output_mode = "append"  # "append", "complete", "update"
        self.checkpoint_dir = "/tmp/trustscoring_checkpoints"
        self.output_format = "parquet"  # "console", "parquet", "csv"
        self.output_path = "data/output/trust_scores_streaming"
        
        # Processing options
        self.watermark_delay = "10 minutes"  # Allow 10 minutes of late data
        self.trigger_interval = "5 seconds"  # Process every 5 seconds
        
        # Model options
        self.model_path = "models/isolation_forest_model.pkl"
        
        # Source reputation window
        self.reputation_window = "1 hour"
        self.reputation_slide = "10 minutes"
        
        # Include intermediate metrics in output?
        self.include_intermediate_metrics = False


def train_initial_model(spark, csv_path: str, model_path: str = "models/isolation_forest_model.pkl"):
    """
    Train initial Isolation Forest model on historical data.
    This must be done once before starting the streaming pipeline.
    
    Args:
        spark: SparkSession
        csv_path: Path to CSV file with historical data
        model_path: Where to save the model
    """
    logger.info("Training initial anomaly detection model...")
    
    if os.path.exists(model_path):
        logger.info(f"Model already exists at {model_path}, skipping training")
        return
    
    # Load historical data
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    # Train model
    model = AnomalyDetectionModel.train_model(
        df,
        sample_fraction=0.1,
        contamination=0.05,
        output_path=model_path
    )
    logger.info(f"Model trained and saved to {model_path}")


def create_streaming_pipeline(spark, config: StreamingPipelineConfig):
    """
    Create the complete streaming pipeline.
    
    Args:
        spark: SparkSession
        config: StreamingPipelineConfig object
        
    Returns:
        StreamingQuery object
    """
    
    logger.info("="*70)
    logger.info("Starting Streaming Data Trust Scoring Pipeline")
    logger.info("="*70)
    
    try:
        # ============================================================
        # Phase 1: Load and parse Kafka stream
        # ============================================================
        logger.info("\nPhase 1: Loading Kafka stream...")
        
        df = load_and_parse_kafka(
            spark,
            bootstrap_servers=config.kafka_bootstrap_servers,
            topic=config.kafka_topic,
            schema=KAFKA_INPUT_SCHEMA,
            starting_offset=config.starting_offset
        )
        
        # Add watermark for late data handling
        logger.info(f"Adding watermark: {config.watermark_delay}")
        df = df.withWatermark("timestamp", config.watermark_delay)
        
        # ============================================================
        # Phase 2: Compute quality metrics
        # ============================================================
        logger.info("\nPhase 2: Computing quality metrics...")
        df = compute_quality_metrics_streaming(df)
        
        # ============================================================
        # Phase 3: Anomaly detection
        # ============================================================
        logger.info("\nPhase 3: Detecting anomalies...")
        df = detect_anomalies_streaming(df, model_path=config.model_path)
        
        # ============================================================
        # Phase 4: Source reputation
        # ============================================================
        logger.info("\nPhase 4: Computing source reputation...")
        df = compute_source_reputation_streaming(
            df,
            window_duration=config.reputation_window,
            sliding_duration=config.reputation_slide
        )
        
        # ============================================================
        # Phase 5: Trust score
        # ============================================================
        logger.info("\nPhase 5: Computing final trust scores...")
        df = compute_trust_score_streaming(df)
        
        # ============================================================
        # Phase 6: Prepare output
        # ============================================================
        logger.info("\nPhase 6: Preparing output columns...")
        df = prepare_output_columns(df, include_intermediate=config.include_intermediate_metrics)
        
        # ============================================================
        # Phase 7: Write to sink
        # ============================================================
        logger.info(f"\nPhase 7: Starting streaming write to {config.output_format}...")
        
        query = create_output_sink(
            df,
            output_format=config.output_format,
            output_path=config.output_path,
            checkpoint_dir=config.checkpoint_dir,
            output_mode=config.output_mode,
            trigger_interval=config.trigger_interval
        )
        
        logger.info("\nStreaming pipeline started successfully!")
        logger.info(f"Streaming to: {config.output_path}")
        logger.info(f"Checkpoint location: {config.checkpoint_dir}")
        
        return query
    
    except Exception as e:
        logger.error(f"Error creating streaming pipeline: {e}", exc_info=True)
        raise


def create_output_sink(df, 
                      output_format: str = "console",
                      output_path: str = None,
                      checkpoint_dir: str = None,
                      output_mode: str = "append",
                      trigger_interval: str = "10 seconds"):
    """
    Create output sink for streaming results.
    
    Args:
        df: Streaming DataFrame
        output_format: "console", "parquet", or "csv"
        output_path: Path for file-based output
        checkpoint_dir: Checkpoint directory for fault tolerance
        output_mode: "append", "complete", or "update"
        trigger_interval: Processing trigger interval
        
    Returns:
        StreamingQuery object
    """
    
    query_builder = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=trigger_interval)
    
    if checkpoint_dir:
        query_builder = query_builder.option("checkpointLocation", checkpoint_dir)
    
    if output_format == "console":
        logger.info("Output mode: Console (first 20 rows)")
        query = query_builder.format("console") \
            .option("numRows", 20) \
            .option("truncate", False) \
            .start()
    
    elif output_format == "parquet":
        logger.info(f"Output mode: Parquet ({output_path})")
        os.makedirs(output_path, exist_ok=True)
        query = query_builder.format("parquet") \
            .option("path", output_path) \
            .start()
    
    elif output_format == "csv":
        logger.info(f"Output mode: CSV ({output_path})")
        os.makedirs(output_path, exist_ok=True)
        query = query_builder.format("csv") \
            .option("path", output_path) \
            .option("header", "true") \
            .start()
    
    else:
        raise ValueError(f"Unknown output format: {output_format}")
    
    return query


def main():
    """Main entry point for streaming pipeline."""
    
    # Create Spark session with Kafka support
    spark = create_spark_session()
    
    # Load configuration
    config = StreamingPipelineConfig()
    
    # Train initial model if needed
    csv_path = "DataTrustFramework/data/raw/transactions.csv"
    if os.path.exists(csv_path):
        train_initial_model(spark, csv_path, model_path=config.model_path)
    else:
        logger.warning(f"CSV file not found: {csv_path}")
        logger.warning("Skipping initial model training")
    
    # Create and start streaming pipeline
    try:
        query = create_streaming_pipeline(spark, config)
        
        # Keep pipeline running
        logger.info("\n" + "="*70)
        logger.info("Pipeline is running. Press Ctrl+C to stop.")
        logger.info("="*70 + "\n")
        
        query.awaitTermination()
    
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
    finally:
        spark.stop()
        logger.info("Spark session closed")


if __name__ == '__main__':
    main()

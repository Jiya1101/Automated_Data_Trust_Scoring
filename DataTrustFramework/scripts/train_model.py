"""
Training script for initial Isolation Forest model.
Must be run BEFORE starting the streaming pipeline.

Usage:
    python train_model.py [--input <csv_path>] [--output <model_path>]
"""

import argparse
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.pipeline.spark_session import create_spark_session
from src.streaming.anomaly_detection_streaming import AnomalyDetectionModel
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Train anomaly detection model')
    parser.add_argument(
        '--input',
        type=str,
        default='DataTrustFramework/data/raw/transactions.csv',
        help='Path to CSV file for training'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='models/isolation_forest_model.pkl',
        help='Path to save trained model'
    )
    parser.add_argument(
        '--sample-fraction',
        type=float,
        default=0.1,
        help='Fraction of data to use for training'
    )
    parser.add_argument(
        '--contamination',
        type=float,
        default=0.05,
        help='Expected contamination ratio'
    )
    
    args = parser.parse_args()
    
    logger.info("="*70)
    logger.info("Training Isolation Forest Model")
    logger.info("="*70)
    logger.info(f"Input: {args.input}")
    logger.info(f"Output: {args.output}")
    logger.info(f"Sample Fraction: {args.sample_fraction}")
    logger.info(f"Contamination: {args.contamination}")
    
    # Check if input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        return 1
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Load data
        logger.info(f"\nLoading data from {args.input}...")
        df = spark.read.csv(args.input, header=True, inferSchema=True)
        logger.info(f"Data shape: {df.count()} rows")
        
        # Train model
        logger.info("\nTraining model...")
        model = AnomalyDetectionModel.train_model(
            df,
            sample_fraction=args.sample_fraction,
            contamination=args.contamination,
            output_path=args.output
        )
        
        logger.info(f"\n✅ Model training complete!")
        logger.info(f"Model saved to: {args.output}")
        logger.info("\nYou can now start the streaming pipeline:")
        logger.info("  python DataTrustFramework/src/pipeline/main_pipeline_streaming.py")
        
        spark.stop()
        return 0
    
    except Exception as e:
        logger.error(f"Training failed: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())

"""
Local Demo Mode - Data Trust Scoring Framework
Simulates streaming pipeline without Docker/Kafka for testing and demonstration.

Usage:
    python local_demo.py [--records 1000] [--delay 0.1] [--output-format parquet]
"""

import os
import sys
import time
import argparse
import logging
from typing import Optional

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'DataTrustFramework'))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.dirname(__file__))

from src.pipeline.spark_session import create_spark_session
from src.quality_metrics.compute_metrics import compute_quality_metrics
from src.anomaly_detection.isolation_forest import detect_anomalies
from src.reputation.source_reputation import compute_source_reputation
from src.trust_score.trust_score import compute_trust_score
from pyspark.sql.functions import current_timestamp, count, avg, round as spark_round, min as spark_min, max as spark_max

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LocalDemoMode:
    """Simulates streaming pipeline locally without Kafka."""
    
    def __init__(self, csv_path: str = "DataTrustFramework/data/raw/transactions.csv",
                 output_path: str = "data/output/trust_scores_demo",
                 max_records: int = 1000,
                 delay: float = 0.0,
                 output_format: str = "parquet"):
        """
        Initialize local demo mode.
        
        Args:
            csv_path: Path to CSV data
            output_path: Output directory
            max_records: Max records to process
            delay: Delay between batches (for demo effect)
            output_format: "parquet", "csv", or "console"
        """
        self.csv_path = csv_path
        self.output_path = output_path
        self.max_records = max_records
        self.delay = delay
        self.output_format = output_format
        self.spark = None
        self.total_processed = 0
    
    def run(self):
        """Execute the complete pipeline."""
        
        try:
            logger.info("="*70)
            logger.info("🚀 Local Demo Mode - Data Trust Scoring Pipeline")
            logger.info("="*70)
            logger.info(f"CSV: {self.csv_path}")
            logger.info(f"Max Records: {self.max_records}")
            logger.info(f"Output Format: {self.output_format}")
            logger.info(f"Output Path: {self.output_path}")
            logger.info("="*70 + "\n")
            
            # Initialize Spark
            logger.info("Initializing Spark...")
            self.spark = create_spark_session()
            
            # Phase 1: Load data
            logger.info("\n📖 Phase 1: Loading CSV Data...")
            start_time = time.time()
            
            if not os.path.exists(self.csv_path):
                logger.error(f"CSV file not found: {self.csv_path}")
                return False
            
            df = self.spark.read.csv(self.csv_path, header=True, inferSchema=True)
            
            # Limit records if specified
            if self.max_records:
                df = df.limit(self.max_records)
            
            record_count = df.count()
            self.total_processed = record_count
            logger.info(f"✓ Loaded {record_count} records from CSV")
            logger.info(f"  Columns: {', '.join(df.columns)}")
            
            # Add ingestion timestamp
            from pyspark.sql.functions import current_timestamp
            df = df.withColumn("ingestion_timestamp", current_timestamp())
            logger.info("✓ Added ingestion_timestamp column")
            
            # Phase 2: Quality Metrics
            logger.info("\n📊 Phase 2: Computing Quality Metrics...")
            phase_start = time.time()
            df = compute_quality_metrics(df)
            phase_time = time.time() - phase_start
            logger.info(f"✓ Quality metrics computed in {phase_time:.2f}s")
            logger.info(f"  Metrics: completeness, consistency, accuracy, freshness, timeliness")
            
            # Phase 3: Anomaly Detection
            logger.info("\n🔍 Phase 3: Detecting Anomalies...")
            phase_start = time.time()
            
            # For local demo, use a smaller sample for training
            sample_fraction = min(0.1, 10000 / record_count) if record_count > 10000 else 0.1
            logger.info(f"  Training on {sample_fraction*100:.1f}% sample...")
            
            df = detect_anomalies(df, sample_fraction=sample_fraction)
            phase_time = time.time() - phase_start
            logger.info(f"✓ Anomaly detection completed in {phase_time:.2f}s")
            
            anomaly_count = df.filter(df.is_anomaly == True).count()
            logger.info(f"  Found {anomaly_count} anomalies ({anomaly_count/record_count*100:.1f}%)")
            
            # Phase 4: Source Reputation
            logger.info("\n⭐ Phase 4: Computing Source Reputation...")
            phase_start = time.time()
            df = compute_source_reputation(df)
            phase_time = time.time() - phase_start
            logger.info(f"✓ Source reputation computed in {phase_time:.2f}s")
            
            # Show reputation by source
            from pyspark.sql.functions import round as spark_round
            rep_summary = df.groupBy("source_id").agg(
                spark_round(spark_round(avg("source_reputation"), 2), 2).alias("avg_reputation"),
                count("*").alias("record_count")
            ).orderBy("source_id")
            logger.info("  Reputation by source:")
            for row in rep_summary.collect():
                logger.info(f"    {row['source_id']}: {row['avg_reputation']} (records: {row['record_count']})")
            
            # Phase 5: Trust Score
            logger.info("\n🎯 Phase 5: Computing Final Trust Scores...")
            phase_start = time.time()
            df = compute_trust_score(df)
            phase_time = time.time() - phase_start
            logger.info(f"✓ Trust scores computed in {phase_time:.2f}s")
            
            # Select final columns
            final_columns = [
                "user_id", "source_id", "purchase_amount", "city", "timestamp",
                "completeness", "consistency", "accuracy", "freshness", "timeliness",
                "is_anomaly", "anomaly_severity",
                "source_reputation", "temporal_decay", "trust_score"
            ]
            
            # Only select columns that exist
            existing_cols = [col for col in final_columns if col in df.columns]
            df_final = df.select(*existing_cols)
            
            # Phase 6: Output
            logger.info(f"\n💾 Phase 6: Writing Output ({self.output_format})...")
            phase_start = time.time()
            
            os.makedirs(self.output_path, exist_ok=True)
            
            if self.output_format == "console":
                logger.info("✓ Sample output (first 20 rows):")
                df_final.show(20, truncate=False)
            
            elif self.output_format == "parquet":
                df_final.coalesce(1).write.mode("overwrite").parquet(
                    os.path.join(self.output_path, "results.parquet")
                )
                phase_time = time.time() - phase_start
                logger.info(f"✓ Results saved to: {self.output_path}/results.parquet")
            
            elif self.output_format == "csv":
                df_final.coalesce(1).write.mode("overwrite").csv(
                    os.path.join(self.output_path, "results.csv"),
                    header=True
                )
                phase_time = time.time() - phase_start
                logger.info(f"✓ Results saved to: {self.output_path}/results.csv")
            
            # Summary Statistics
            logger.info("\n📈 Summary Statistics:")
            logger.info(f"  Total records processed: {record_count}")
            
            # Trust score distribution
            from pyspark.sql.functions import min as spark_min, max as spark_max
            stats = df_final.select(
                spark_min("trust_score").alias("min_score"),
                spark_max("trust_score").alias("max_score"),
                round(avg("trust_score"), 2).alias("avg_score")
            ).collect()[0]
            
            logger.info(f"  Trust Score Range: {stats['min_score']:.1f} - {stats['max_score']:.1f}")
            logger.info(f"  Average Trust Score: {stats['avg_score']}")
            
            # Quality metrics summary
            quality_summary = df_final.select(
                round(avg("completeness"), 2).alias("avg_completeness"),
                round(avg("consistency"), 2).alias("avg_consistency"),
                round(avg("accuracy"), 2).alias("avg_accuracy"),
                round(avg("freshness"), 2).alias("avg_freshness"),
                round(avg("timeliness"), 2).alias("avg_timeliness")
            ).collect()[0]
            
            logger.info(f"  Average Quality Metrics:")
            logger.info(f"    Completeness: {quality_summary['avg_completeness']}")
            logger.info(f"    Consistency: {quality_summary['avg_consistency']}")
            logger.info(f"    Accuracy: {quality_summary['avg_accuracy']}")
            logger.info(f"    Freshness: {quality_summary['avg_freshness']}")
            logger.info(f"    Timeliness: {quality_summary['avg_timeliness']}")
            
            total_time = time.time() - start_time
            logger.info(f"\n✅ Pipeline completed in {total_time:.2f}s")
            logger.info(f"   Throughput: {record_count/total_time:.0f} records/sec")
            
            logger.info("\n" + "="*70)
            logger.info("🎉 Demo completed successfully!")
            logger.info("="*70)
            
            return True
        
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            return False
        
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("\nSpark session closed")


def main():
    parser = argparse.ArgumentParser(
        description='Local Demo Mode - Test Data Trust Pipeline Without Docker'
    )
    parser.add_argument(
        '--records',
        type=int,
        default=1000,
        help='Maximum records to process (default: 1000)'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.0,
        help='Delay between phases in seconds (for demo effect)'
    )
    parser.add_argument(
        '--output-format',
        type=str,
        choices=['parquet', 'csv', 'console'],
        default='parquet',
        help='Output format (default: parquet)'
    )
    parser.add_argument(
        '--csv-file',
        type=str,
        default='DataTrustFramework/data/raw/transactions.csv',
        help='Path to CSV input file'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/output/trust_scores_demo',
        help='Output directory'
    )
    
    args = parser.parse_args()
    
    demo = LocalDemoMode(
        csv_path=args.csv_file,
        output_path=args.output_dir,
        max_records=args.records,
        delay=args.delay,
        output_format=args.output_format
    )
    
    success = demo.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()

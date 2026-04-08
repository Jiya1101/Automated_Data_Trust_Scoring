"""
Simplified Local Demo Mode - Data Trust Scoring Framework
Runs the complete pipeline without Docker/Kafka.
"""

import os
import sys
import time
import argparse

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'DataTrustFramework'))

if not os.path.exists('DataTrustFramework/data/raw/transactions.csv'):
    print("ERROR: transactions.csv not found!")
    print("Please generate data first: python DataTrustFramework/scripts/generate_dataset.py")
    sys.exit(1)

# Use custom minimal spark setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, avg, round as spark_round, count, min as spark_min, max as spark_max

# Create minimal Spark session
spark = SparkSession.builder.appName("DataTrustDemo").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from src.quality_metrics.compute_metrics import compute_quality_metrics
from src.anomaly_detection.isolation_forest import detect_anomalies
from src.reputation.source_reputation import compute_source_reputation
from src.trust_score.trust_score import compute_trust_score

def run_demo(records=1000, output_format="console"):
    """Run the complete pipeline."""
    print("\n" + "="*70)
    print("🚀 Local Demo Mode - Data Trust Scoring Pipeline")
    print("="*70 + "\n")
    
    try:
        # Phase 1: Load
        print("📖 Phase 1: Loading CSV Data...")
        df = spark.read.csv('DataTrustFramework/data/raw/transactions.csv', header=True, inferSchema=True)
        if records:
            df = df.limit(records)
        record_count = df.count()
        print(f"✓ Loaded {record_count} records")
        
        # Add ingestion timestamp
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        # Phase 2: Quality Metrics
        print("\n📊 Phase 2: Computing Quality Metrics...")
        start = time.time()
        df = compute_quality_metrics(df)
        print(f"✓ Completed in {time.time()-start:.1f}s")
        
        # Phase 3: Anomaly Detection
        print("\n🔍 Phase 3: Detecting Anomalies...")
        start = time.time()
        sample_frac = min(0.1, 10000/record_count) if record_count > 10000 else 0.1
        df = detect_anomalies(df, sample_fraction=sample_frac)
        print(f"✓ Completed in {time.time()-start:.1f}s")
        
        anomaly_count = df.filter(df.is_anomaly == True).count()
        print(f"  Found {anomaly_count} anomalies ({anomaly_count/record_count*100:.1f}%)")
        
        # Phase 4: Source Reputation
        print("\n⭐ Phase 4: Computing Source Reputation...")
        start = time.time()
        df = compute_source_reputation(df)
        print(f"✓ Completed in {time.time()-start:.1f}s")
        
        # Phase 5: Trust Score
        print("\n🎯 Phase 5: Computing Final Trust Scores...")
        start = time.time()
        df = compute_trust_score(df)
        print(f"✓ Completed in {time.time()-start:.1f}s")
        
        # Select final columns
        final_cols = [c for c in [
            "user_id", "source_id", "purchase_amount", "city", "timestamp",
            "completeness", "consistency", "accuracy", "freshness", "timeliness",
            "is_anomaly", "source_reputation", "trust_score"
        ] if c in df.columns]
        
        df_final = df.select(*final_cols)
        
        # Phase 6: Output
        print(f"\n💾 Phase 6: Writing Output...")
        os.makedirs("data/output/trust_scores_demo", exist_ok=True)
        
        if output_format == "console":
            print("\n✓ Sample Results (first 20 rows):\n")
            df_final.show(20, truncate=False)
        elif output_format == "parquet":
            df_final.coalesce(1).write.mode("overwrite").parquet("data/output/trust_scores_demo/results.parquet")
            print("✓ Saved to: data/output/trust_scores_demo/results.parquet")
        
        # Summary
        print("\n📈 Summary Statistics:")
        stats = df_final.select(
            spark_min("trust_score").alias("min"),
            spark_max("trust_score").alias("max"),
            spark_round(avg("trust_score"), 2).alias("avg")
        ).collect()[0]
        
        print(f"  Trust Score Range: {stats['min']:.1f} - {stats['max']:.1f}")
        print(f"  Average Trust Score: {stats['avg']}")
        
        print("\n" + "="*70)
        print("✅ Pipeline completed successfully!")
        print("="*70 + "\n")
        
        return True
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}\n", flush=True)
        import traceback
        traceback.print_exc()
        return False
    finally:
        spark.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Local Demo - Data Trust Scoring')
    parser.add_argument('--records', type=int, default=5000, help='Records to process')
    parser.add_argument('--output', type=str, choices=['console', 'parquet'], default='console')
    
    args = parser.parse_args()
    success = run_demo(records=args.records, output_format=args.output)
    sys.exit(0 if success else 1)

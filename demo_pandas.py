"""
Pure Pandas Demo - Data Trust Scoring Pipeline (No Spark/Java Required)
Simulates the complete pipeline using pandas for quick demonstration.

Usage:
    python demo_pandas.py --records 5000
"""

import sys
import os
import time
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

def run_demo(records=5000):
    """Run the complete trust scoring pipeline with pandas."""
    
    start_total = time.time()
    
    print("\n" + "="*80)
    print("🚀 LOCAL DEMO MODE - Data Trust Scoring Pipeline (Pandas)")
    print("="*80)
    print(f"Mode: Pure Pandas (No Spark/Docker required)")
    print(f"Records: {records:,}")
    print("="*80 + "\n")
    
    try:
        # PHASE 1: Load Data
        print("📖 Phase 1: Loading Transaction Data...")
        csv_path = "DataTrustFramework/data/raw/transactions.csv"
        
        if not os.path.exists(csv_path):
            print(f"❌ CSV file not found: {csv_path}")
            print("Please generate data first:")
            print("  python DataTrustFramework/scripts/generate_dataset.py")
            return False
        
        p1_start = time.time()
        df = pd.read_csv(csv_path, nrows=records)
        print(f"✓ Loaded {len(df):,} records in {time.time()-p1_start:.2f}s")
        print(f"  Columns: {', '.join(df.columns)}")
        print(f"  Memory: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        
        # Add ingestion timestamp
        df['ingestion_timestamp'] = datetime.now()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # PHASE 2: Quality Metrics
        print("\n📊 Phase 2: Computing Data Quality Metrics...")
        p2_start = time.time()
        
        # Completeness: % of non-null fields per record
        total_fields = 5.0
        df['completeness'] = (
            (~df['user_id'].isnull()).astype(int) +
            (~df['purchase_amount'].isnull()).astype(int) +
            (~df['city'].isnull()).astype(int) +
            (~df['timestamp'].isnull()).astype(int) +
            (~df['source_id'].isnull()).astype(int)
        ) / total_fields
        
        # Consistency: valid amounts (not null)
        df['consistency'] = (~df['purchase_amount'].isnull()).astype(float)
        
        # Accuracy: amount within 0-10000 range
        df['accuracy'] = ((df['purchase_amount'] >= 0) & (df['purchase_amount'] <= 10000)).astype(float)
        
        # Freshness: based on data age
        days_old = (datetime.now() - df['timestamp']).dt.days
        df['freshness'] = np.clip(1.0 - (days_old / 30.0), 0, 1)
        
        # Timeliness: based on ingestion delay
        arrival_delay = (df['ingestion_timestamp'] - df['timestamp']).dt.days
        df['timeliness'] = np.where(
            arrival_delay <= 7, 1.0,
            np.where(arrival_delay >= 45, 0.0, 1.0 - ((arrival_delay - 7) / 38.0))
        )
        
        print(f"✓ Metrics computed in {time.time()-p2_start:.2f}s")
        print(f"  Completeness: avg={df['completeness'].mean():.2f}, min={df['completeness'].min():.2f}, max={df['completeness'].max():.2f}")
        print(f"  Consistency:  avg={df['consistency'].mean():.2f}, min={df['consistency'].min():.2f}, max={df['consistency'].max():.2f}")
        print(f"  Accuracy:     avg={df['accuracy'].mean():.2f}, min={df['accuracy'].min():.2f}, max={df['accuracy'].max():.2f}")
        
        # PHASE 3: Anomaly Detection
        print("\n🔍 Phase 3: Detecting Anomalies with Isolation Forest...")
        p3_start = time.time()
        
        # Train on purchase_amount
        X = df[['purchase_amount']].fillna(0).values
        iso_forest = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
        predictions = iso_forest.fit_predict(X)
        scores = iso_forest.score_samples(X)
        
        df['is_anomaly'] = (predictions == -1)
        df['anomaly_severity'] = -scores  # Invert so higher = more anomalous
        
        anomaly_count = df['is_anomaly'].sum()
        print(f"✓ Anomaly detection completed in {time.time()-p3_start:.2f}s")
        print(f"  Found {anomaly_count:,} anomalies ({anomaly_count/len(df)*100:.1f}%)")
        print(f"  Anomaly Severity: avg={df[df['is_anomaly']]['anomaly_severity'].mean():.2f}")
        
        # PHASE 4: Source Reputation
        print("\n⭐ Phase 4: Computing Source Reputation...")
        p4_start = time.time()
        
        # Calculate reputation per source: 1 - (anomalies/total)
        source_stats = df.groupby('source_id').agg({
            'is_anomaly': ['sum', 'count']
        })
        source_stats.columns = ['anomaly_count', 'total']
        source_stats['reputation'] = 1.0 - (source_stats['anomaly_count'] / source_stats['total'])
        
        # Map back to rows
        df['source_reputation'] = df['source_id'].map(source_stats['reputation'])
        
        print(f"✓ Source reputation computed in {time.time()-p4_start:.2f}s")
        for source_id, rep in source_stats['reputation'].items():
            total = int(source_stats.loc[source_id, 'total'])
            anomalies = int(source_stats.loc[source_id, 'anomaly_count'])
            print(f"  {source_id}: reputation={rep:.3f} (anomalies: {anomalies}/{total})")
        
        # PHASE 5: Temporal Decay & Trust Score
        print("\n🎯 Phase 5: Computing Final Trust Scores...")
        p5_start = time.time()
        
        # Temporal decay: exp(-0.05 * days_since_ingest)
        days_since_ingest = (datetime.now() - df['ingestion_timestamp']).dt.days
        df['temporal_decay'] = np.exp(-0.05 * days_since_ingest.astype(float))
        
        # Trust score = (avg quality metrics) * reputation * temporal_decay * 100
        quality_avg = (
            df['completeness'] + 
            df['consistency'] + 
            df['accuracy'] + 
            df['freshness'] + 
            df['timeliness']
        ) / 5.0
        
        df['trust_score'] = (
            quality_avg * 
            df['source_reputation'].fillna(0.5) * 
            df['temporal_decay']
        ) * 100.0
        
        print(f"✓ Trust scores computed in {time.time()-p5_start:.2f}s")
        print(f"  Score Range: {df['trust_score'].min():.1f} - {df['trust_score'].max():.1f}")
        print(f"  Average Score: {df['trust_score'].mean():.1f}")
        print(f"  Median Score: {df['trust_score'].median():.1f}")
        
        # PHASE 6: Output & Summary
        print("\n💾 Phase 6: Preparing Output...")
        
        # Select final columns
        output_cols = [
            'user_id', 'source_id', 'purchase_amount', 'city', 'timestamp',
            'completeness', 'consistency', 'accuracy', 'freshness', 'timeliness',
            'is_anomaly', 'anomaly_severity', 'source_reputation', 'temporal_decay', 'trust_score'
        ]
        df_output = df[[col for col in output_cols if col in df.columns]]
        
        # Display sample
        print("\n✓ Sample Results (first 20 rows):\n")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 30)
        print(df_output.head(20).to_string())
        
        # Save to parquet
        os.makedirs("data/output/trust_scores_demo", exist_ok=True)
        df_output.to_parquet("data/output/trust_scores_demo/results_demo.parquet")
        print(f"\n✓ Saved results to: data/output/trust_scores_demo/results_demo.parquet")
        
        # Save to CSV
        df_output.to_csv("data/output/trust_scores_demo/results_demo.csv", index=False)
        print(f"✓ Saved results to: data/output/trust_scores_demo/results_demo.csv")
        
        # Final Summary
        print("\n" + "="*80)
        print("📊 PIPELINE SUMMARY")
        print("="*80)
        print(f"Total Records Processed: {len(df):,}")
        print(f"Total Execution Time: {time.time()-start_total:.2f}s")
        print(f"Throughput: {len(df)/(time.time()-start_total):,.0f} records/sec\n")
        
        print("Quality Metrics Summary:")
        print(f"  Completeness: {df['completeness'].mean():.3f}")
        print(f"  Consistency:  {df['consistency'].mean():.3f}")
        print(f"  Accuracy:     {df['accuracy'].mean():.3f}")
        print(f"  Freshness:    {df['freshness'].mean():.3f}")
        print(f"  Timeliness:   {df['timeliness'].mean():.3f}\n")
        
        print("Anomaly Detection Results:")
        print(f"  Total Anomalies: {df['is_anomaly'].sum():,} ({df['is_anomaly'].mean()*100:.1f}%)")
        print(f"  Anomaly Severity: avg={df[df['is_anomaly']]['anomaly_severity'].mean():.2f}\n")
        
        print("Trust Score Distribution:")
        for percentile in [0, 25, 50, 75, 100]:
            value = df['trust_score'].quantile(percentile/100)
            print(f"  {percentile:3d}th percentile: {value:6.1f}")
        
        print("\n" + "="*80)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*80 + "\n")
        
        return True
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        import traceback
        traceback.print_exc()
        return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Local Demo Mode - Test Data Trust Pipeline (Pandas)'
    )
    parser.add_argument(
        '--records',
        type=int,
        default=5000,
        help='Number of records to process (default: 5000)'
    )
    
    args = parser.parse_args()
    success = run_demo(records=args.records)
    sys.exit(0 if success else 1)

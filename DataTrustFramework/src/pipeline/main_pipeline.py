import os
import sys
import time
import psutil

# Add project root to sys.path to allow imports from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.pipeline.spark_session import create_spark_session
from src.ingestion.load_data import load_csv_data
from src.quality_metrics.compute_metrics import compute_quality_metrics
from src.anomaly_detection.isolation_forest import detect_anomalies
from src.reputation.source_reputation import compute_source_reputation
from src.trust_score.trust_score import compute_trust_score
from src.visualization.plots import generate_visualizations

def main():
    start_time = time.time()
    
    print("="*60)
    print("Starting Automated Data Trust Scoring Framework Pipeline")
    print("="*60)
    
    # 1. Initialize SparkSession
    spark = create_spark_session()
    
    # Define file paths
    unified_data_path = 'DataTrustFramework/data/unified/combined_real_data'
    delta_output_path = 'data/output/trust_scores'
    
    # 2. Load Dataset using Pandas to bypass missing Hadoop DLLs natively on Windows
    print("\nPhase 1: Ingestion (Pandas -> Spark)")
    import pandas as pd
    pdf_in = pd.read_parquet(unified_data_path)
    # SAMPLE down to 20,000 records to bypass local machine JVM memory limits smoothly
    pdf_in = pdf_in.sample(n=min(20000, len(pdf_in)), random_state=42)
    df = spark.createDataFrame(pdf_in)
    
    # 3. Compute Quality Metrics
    print("\nPhase 2: Quality Metrics Computation")
    from pyspark.sql.functions import current_timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = compute_quality_metrics(df)
    
    # 4. Anomaly Detection
    print("\nPhase 3: Anomaly Detection via Isolation Forest")
    # Sample 5% for quick model training in this example
    df = detect_anomalies(df, sample_fraction=0.05)
    
    # 5. Compute Reputation
    print("\nPhase 4: Source Reputation Calculation")
    df = compute_source_reputation(df)
    
    # 6. Apply Temporal Decay and Compute Trust Score
    print("\nPhase 5: Computing Final Trust Score & Applying Temporal Decay")
    df = compute_trust_score(df)
    
    # Select final requested columns
    final_columns = [
        "user_id", "source_id", "completeness", "consistency", "accuracy", 
        "freshness", "timeliness", "is_anomaly", "source_reputation", 
        "temporal_decay", "trust_score"
    ]
    
    final_df = df.select(*final_columns)
    
    # 7. Parquet Storage
    print(f"\nPhase 6: Writing Output to Parquet at {delta_output_path}")
    pdf = final_df.toPandas()
    os.makedirs(delta_output_path, exist_ok=True)
    pdf.to_parquet(f"{delta_output_path}/trust_scores.parquet", index=False)

    
    
    # Count processed records
    records_processed = final_df.count()
    
    # 8. Visualization
    print("\nPhase 7: Visualizing Results")
    # To avoid memory issues with huge datasets, sample down for visualization if necessary
    # or just collect the required columns.
    sample_size_for_viz = min(100_000, records_processed)
    fraction = sample_size_for_viz / records_processed if records_processed > 0 else 1.0
    
    pdf = final_df.sample(fraction=fraction).select("trust_score", "source_id", "is_anomaly", "source_reputation").toPandas()
    generate_visualizations(pdf)
    
    # 9. Performance Metrics
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Get basic memory usage of current process
    process = psutil.Process()
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / (1024 * 1024)
    
    print("\n" + "="*60)
    print("Pipeline Execution Summary")
    print("="*60)
    print(f"Total Records Processed: {records_processed:,}")
    print(f"Total Execution Time:    {execution_time:.2f} seconds")
    print(f"Driver Memory Usage:     {memory_mb:.2f} MB")
    print("="*60)
    print("Pipeline completed successfully!")
    
    spark.stop()

if __name__ == "__main__":
    main()

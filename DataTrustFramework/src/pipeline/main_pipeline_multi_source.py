"""
main_pipeline_multi_source.py
------------------------------
End-to-end Data Trust Scoring Pipeline that ingests REAL multi-source datasets.

Data sources (ALL REAL CSV FILES - no synthetic data):
    - DataTrustFramework/data/raw/transactions.csv
    - DataTrustFramework/data/raw/multi_source/ecommerce_data.csv
    - DataTrustFramework/data/raw/multi_source/subscription_data.csv
    - DataTrustFramework/data/raw/multi_source/taxi_data.csv

Unified schema: user_id | amount | city | timestamp | source_id

Pipeline stages:
    1. Multi-source ingestion & schema unification
    2. Quality metrics (completeness, consistency, accuracy, freshness, timeliness)
    3. Anomaly detection (Isolation Forest via Pandas UDF)
    4. Source reputation scoring
    5. Trust score with temporal decay
    6. Parquet output + visualizations + summary
"""

import os
import sys
import time
import psutil

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.pipeline.spark_session            import create_spark_session
from src.ingestion.multi_source_loader     import load_all_sources, validate_and_show
from src.quality_metrics.compute_metrics   import compute_quality_metrics
from src.anomaly_detection.isolation_forest import detect_anomalies
from src.reputation.source_reputation      import compute_source_reputation
from src.trust_score.trust_score           import compute_trust_score
from src.visualization.plots               import generate_visualizations


UNIFIED_PARQUET_PATH  = "DataTrustFramework/data/unified/combined_real_data"
TRUST_SCORE_OUT_PATH  = "DataTrustFramework/data/output/trust_scores_multi_source"


def main():
    start_time = time.time()

    print("=" * 70)
    print("  Data Trust Scoring Framework — REAL Multi-Source Pipeline")
    print("=" * 70)
    print("  Sources: transactions | ecommerce | subscription | taxi")
    print("  NO synthetic data — all records originate from real CSV files")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 1. Spark Session
    # ------------------------------------------------------------------
    spark = create_spark_session(app_name="DataTrustScoring_MultiSource")

    # ------------------------------------------------------------------
    # 2. Load & unify all real datasets
    # ------------------------------------------------------------------
    print("\n[Phase 1] Multi-Source Ingestion & Schema Unification")
    df = load_all_sources(spark)

    # Checkpoint validation
    validate_and_show(df)

    # ------------------------------------------------------------------
    # 3. Save unified dataset to Parquet (intermediate checkpoint)
    # ------------------------------------------------------------------
    print(f"\n[Phase 1b] Saving unified dataset -> {UNIFIED_PARQUET_PATH}")
    df.write.mode("overwrite").parquet(UNIFIED_PARQUET_PATH)
    print("  Unified parquet saved.")

    # Reload from parquet for deterministic downstream processing
    df = spark.read.parquet(UNIFIED_PARQUET_PATH)

    # ------------------------------------------------------------------
    # 4. Quality Metrics
    # ------------------------------------------------------------------
    print("\n[Phase 2] Computing Quality Metrics")
    df = compute_quality_metrics(df)

    # ------------------------------------------------------------------
    # 5. Anomaly Detection
    # ------------------------------------------------------------------
    print("\n[Phase 3] Anomaly Detection (Isolation Forest)")
    # Sample 2% for training — balanced across all sources
    df = detect_anomalies(df, sample_fraction=0.02)

    # ------------------------------------------------------------------
    # 6. Source Reputation
    # ------------------------------------------------------------------
    print("\n[Phase 4] Computing Source Reputation Scores")
    df = compute_source_reputation(df)

    # ------------------------------------------------------------------
    # 7. Trust Score + Temporal Decay
    # ------------------------------------------------------------------
    print("\n[Phase 5] Computing Final Trust Score with Temporal Decay")
    df = compute_trust_score(df)

    # ------------------------------------------------------------------
    # 8. Final column selection
    # ------------------------------------------------------------------
    final_columns = [
        "user_id", "source_id", "amount", "city",
        "completeness", "consistency", "accuracy",
        "freshness", "timeliness",
        "is_anomaly", "source_reputation",
        "temporal_decay", "trust_score",
    ]

    final_df = df.select(*final_columns)

    # ------------------------------------------------------------------
    # 9. Parquet Output
    # ------------------------------------------------------------------
    print(f"\n[Phase 6] Writing Trust Score Results -> {TRUST_SCORE_OUT_PATH}")
    pdf = final_df.toPandas()
    os.makedirs(TRUST_SCORE_OUT_PATH, exist_ok=True)
    pdf.to_parquet(
        os.path.join(TRUST_SCORE_OUT_PATH, "trust_scores.parquet"),
        index=False,
    )

    # ------------------------------------------------------------------
    # 10. Sample Output
    # ------------------------------------------------------------------
    print("\n[Trust Score Sample — 10 rows]")
    final_df.show(10, truncate=False)

    print("\n[Breakdown] Average trust score per source:")
    from pyspark.sql.functions import avg, round as spark_round
    final_df.groupBy("source_id").agg(
        spark_round(avg("trust_score"), 2).alias("avg_trust_score"),
        spark_round(avg("completeness"), 3).alias("avg_completeness"),
        spark_round(avg("accuracy"), 3).alias("avg_accuracy"),
    ).orderBy("source_id").show()

    # ------------------------------------------------------------------
    # 11. Record Count
    # ------------------------------------------------------------------
    records_processed = final_df.count()

    # ------------------------------------------------------------------
    # 12. Visualization
    # ------------------------------------------------------------------
    print("\n[Phase 7] Generating Visualizations")
    sample_size = min(100_000, records_processed)
    fraction = sample_size / records_processed if records_processed > 0 else 1.0
    pdf_viz = (
        final_df
        .sample(fraction=fraction)
        .select("trust_score", "source_id", "is_anomaly", "source_reputation")
        .toPandas()
    )
    generate_visualizations(pdf_viz)

    # ------------------------------------------------------------------
    # 13. Execution Summary
    # ------------------------------------------------------------------
    end_time      = time.time()
    execution_sec = end_time - start_time
    memory_mb     = psutil.Process().memory_info().rss / (1024 * 1024)

    print("\n" + "=" * 70)
    print("  Pipeline Execution Summary")
    print("=" * 70)
    print(f"  Total Records Processed : {records_processed:,}")
    print(f"  Total Execution Time    : {execution_sec:.2f} seconds")
    print(f"  Driver Memory Usage     : {memory_mb:.2f} MB")
    print(f"  Unified Parquet         : {UNIFIED_PARQUET_PATH}")
    print(f"  Trust Score Output      : {TRUST_SCORE_OUT_PATH}/trust_scores.parquet")
    print("=" * 70)
    print("  ✔ ALL data from real CSVs — NO synthetic generation")
    print("  ✔ 4 sources: transactions, ecommerce, subscription, taxi")
    print("  ✔ Pipeline completed successfully!")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()

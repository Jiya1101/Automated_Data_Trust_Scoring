"""
Quick Reference: Using Unified Data with Trust Scoring Pipeline
================================================================

This guide shows how to use the newly created unified dataset
with the Data Trust Scoring Framework.
"""

# ============================================================================
# 1. LOAD UNIFIED DATA
# ============================================================================

import pandas as pd
import numpy as np

# Load the unified dataset
unified_data_path = 'DataTrustFramework/data/unified/combined_unified_data.parquet'
df = pd.read_parquet(unified_data_path)

print(f"Loaded {len(df):,} records from {len(df['source_id'].unique())} sources")
print(f"Columns: {df.columns.tolist()}")
print(f"\nDataFrame Info:")
print(df.info())
print(f"\nFirst few records:")
print(df.head(10))

# ============================================================================
# 2. BASIC DATA EXPLORATION
# ============================================================================

# Records by source
print("\nRecords by Source:")
print(df['source_id'].value_counts())

# Amount statistics
print("\nAmount Statistics (USD):")
print(df['amount'].describe())

# Time range
print("\nTime Range:")
print(f"From: {df['timestamp'].min()}")
print(f"To: {df['timestamp'].max()}")

# Missing values
print("\nData Completeness:")
print(df.isnull().sum())
print(f"\nCompleteness Rate: {(1 - df.isnull().sum() / len(df)).mean() * 100:.2f}%")

# ============================================================================
# 3. SOURCE-SPECIFIC ANALYSIS
# ============================================================================

for source in df['source_id'].unique():
    source_df = df[df['source_id'] == source]
    print(f"\n{source.upper()}")
    print(f"  Records: {len(source_df):,}")
    print(f"  Amount Range: ${source_df['amount'].min():.2f} - ${source_df['amount'].max():.2f}")
    print(f"  Mean Amount: ${source_df['amount'].mean():.2f}")
    print(f"  Missing Cities: {source_df['city'].isnull().sum()}")

# ============================================================================
# 4. PREPARE FOR TRUST SCORING
# ============================================================================

from datetime import datetime

# Create a clean version for pipeline
df_clean = df.copy()

# Remove records with null cities (optional)
df_clean = df_clean.dropna(subset=['city'])

# Add derived features
df_clean['amount_log'] = np.log1p(df_clean['amount'])
df_clean['age_days'] = (datetime.now(df_clean['timestamp'].dt.tz) - df_clean['timestamp']).dt.days

print(f"\nClean dataset: {len(df_clean):,} records")
print(f"Removed null cities: {len(df) - len(df_clean):,} records")

# ============================================================================
# 5. USE WITH QUALITY METRICS PIPELINE
# ============================================================================

# Option A: Direct CSV Export
df_clean.to_csv('DataTrustFramework/data/raw/unified_transactions.csv', index=False)
print(f"\n✓ Exported to: DataTrustFramework/data/raw/unified_transactions.csv")

# Option B: Use with Pandas-based Demo
demo_data_path = 'data/output/trust_scores_unified/demo_input.parquet'
df_clean.to_parquet(demo_data_path)
print(f"✓ Exported to: {demo_data_path}")

# ============================================================================
# 6. SAMPLING FOR QUICK TESTING
# ============================================================================

# Get a sample for quick pipeline testing
sample_sizes = [1000, 5000, 10000, 50000, 100000]

for sample_size in sample_sizes:
    if sample_size <= len(df_clean):
        sample_df = df_clean.sample(n=sample_size, random_state=42)
        sample_path = f'DataTrustFramework/data/raw/sample_{sample_size}_records.csv'
        sample_df.to_csv(sample_path, index=False)
        print(f"✓ Created {sample_size:,} record sample: {sample_path}")

# ============================================================================
# 7. STREAM TO KAFKA (Optional)
# ============================================================================

"""
To stream unified data to Kafka:

1. Make sure Kafka is running:
   docker-compose up -d

2. Run the producer:
   cd DataTrustFramework
   python scripts/kafka_producer.py \
       --csv-file ../DataTrustFramework/data/unified/combined_unified_data.csv \
       --topic data_trust_unified \
       --max-records 100000 \
       --delay 0.01

3. The streaming pipeline will consume from the topic:
   python demo_kafka_pandas.py
"""

# ============================================================================
# 8. PIPELINE READY STATISTICS
# ============================================================================

print("\n" + "=" * 60)
print("DATA READY FOR TRUST SCORING PIPELINE")
print("=" * 60)
print(f"✓ Input Records: {len(df):,}")
print(f"✓ Sources: {df['source_id'].nunique()}")
print(f"✓ Columns: {', '.join(df.columns)}")
print(f"✓ Amount Range: ${df['amount'].min():.2f} - ${df['amount'].max():.2f}")
print(f"✓ Time Span: {(df['timestamp'].max() - df['timestamp'].min()).days} days")
print(f"✓ Data Quality: {(1 - df.isnull().sum() / len(df)).mean() * 100:.2f}%")
print("\nNext Steps:")
print("1. Load this data in the trust scoring pipeline")
print("2. Apply quality metrics computation")
print("3. Run anomaly detection")
print("4. Calculate trust scores")
print("5. Analyze results by source")
print("=" * 60)

# ============================================================================
# 9. ADVANCED: TRAIN/TEST SPLIT
# ============================================================================

from sklearn.model_selection import train_test_split

# Create train/test split for model validation
train_df, test_df = train_test_split(df_clean, test_size=0.2, random_state=42)

print(f"\nTrain/Test Split:")
print(f"  Training: {len(train_df):,} records")
print(f"  Testing: {len(test_df):,} records")

# Save splits
train_df.to_parquet('DataTrustFramework/data/unified/train_data.parquet')
test_df.to_parquet('DataTrustFramework/data/unified/test_data.parquet')
print(f"✓ Splits saved to: DataTrustFramework/data/unified/")

# ============================================================================
# 10. EXPORT SUMMARIES
# ============================================================================

# Create metadata file
import json

metadata = {
    "dataset": "Unified Multi-Source Data",
    "total_records": int(len(df)),
    "clean_records": int(len(df_clean)),
    "sources": df['source_id'].unique().tolist(),
    "columns": df.columns.tolist(),
    "amount_stats": {
        "min": float(df['amount'].min()),
        "max": float(df['amount'].max()),
        "mean": float(df['amount'].mean()),
        "median": float(df['amount'].median()),
        "stddev": float(df['amount'].std())
    },
    "time_range": {
        "start": str(df['timestamp'].min()),
        "end": str(df['timestamp'].max())
    },
    "data_quality": {
        "completeness": float((1 - df.isnull().sum() / len(df)).mean()),
        "null_cities": int(df['city'].isnull().sum()),
        "null_amounts": int(df['amount'].isnull().sum()),
        "null_timestamps": int(df['timestamp'].isnull().sum())
    }
}

with open('DataTrustFramework/data/unified/metadata.json', 'w') as f:
    json.dump(metadata, f, indent=2, default=str)

print(f"\n✓ Metadata saved to: DataTrustFramework/data/unified/metadata.json")
print(f"\nMetadata:")
print(json.dumps(metadata, indent=2, default=str))

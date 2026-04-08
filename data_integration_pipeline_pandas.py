"""
Multi-Source Data Integration Pipeline (Pandas Version)
Ingests, cleans, transforms, and combines multiple datasets into a unified format.
Works without PySpark to avoid Java compatibility issues on Windows.

Schema Mapping:
- Source A (Transactions): user_id, purchase_amount → amount, city, timestamp, source_id
- Source B (E-Commerce): order_id → user_id, order_value → amount, location → city, order_date → timestamp
- Source C (Taxi): trip_id → user_id, fare_amount → amount, pickup_location → city, trip_timestamp → timestamp
- Source D (Subscription): subscription_id → user_id, monthly_billing → amount, city_name → city, created_at → timestamp
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
RAW_DATA_DIR = 'DataTrustFramework/data/raw'
OUTPUT_DIR = 'DataTrustFramework/data/unified'


class DataIntegrationPipelinePandas:
    """Multi-source data integration and standardization using Pandas."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    def load_dataset(self, path, name):
        """Load a dataset and log its schema."""
        self.logger.info(f"Loading {name} from {path}")
        df = pd.read_csv(path)
        self.logger.info(f"{name}: {len(df):,} records, {len(df.columns)} columns")
        self.logger.info(f"Columns: {df.columns.tolist()}")
        return df
    
    def standardize_amount(self, df, amount_col):
        """
        Standardize amount column to numeric.
        Handle missing/invalid values.
        """
        df = df.copy()
        df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce')
        # Remove zero and negative amounts
        df[amount_col] = df[amount_col].where(df[amount_col] > 0, None)
        return df.rename(columns={amount_col: 'amount'})
    
    def standardize_timestamp(self, df, timestamp_col):
        """
        Standardize timestamp column to ISO format.
        Handle various timestamp formats.
        """
        df = df.copy()
        try:
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True)
        except:
            try:
                df[timestamp_col] = pd.to_datetime(df[timestamp_col], infer_datetime_format=True)
            except:
                self.logger.warning(f"Could not parse {timestamp_col}, setting to None")
                df[timestamp_col] = None
        return df.rename(columns={timestamp_col: 'timestamp'})
    
    def standardize_city(self, df, city_col):
        """
        Standardize city names.
        Clean up whitespace and case.
        """
        df = df.copy()
        df[city_col] = df[city_col].fillna('Unknown')
        df[city_col] = df[city_col].astype(str).str.strip().str.lower()
        # Clean multiple spaces
        df[city_col] = df[city_col].str.replace(r'\s+', ' ', regex=True)
        return df.rename(columns={city_col: 'city'})
    
    def standardize_user_id(self, df, user_id_col):
        """
        Standardize user_id column.
        Use existing ID or generate one.
        """
        df = df.copy()
        # Convert to string, generate IDs for nulls
        df[user_id_col] = df[user_id_col].fillna('').astype(str)
        null_mask = df[user_id_col] == ''
        df.loc[null_mask, user_id_col] = [f"generated_{i}" for i in range(null_mask.sum())]
        return df.rename(columns={user_id_col: 'user_id'})
    
    def transform_transactions(self, df):
        """Transform transactions dataset (already has correct schema)."""
        self.logger.info("Transforming Transactions dataset...")
        
        df = df.copy()
        
        # Select and rename columns
        df = df[['user_id', 'purchase_amount', 'city', 'timestamp', 'source_id']].copy()
        df = df.rename(columns={'purchase_amount': 'amount'})
        
        # Standardize
        df = df.dropna(subset=['user_id', 'amount', 'timestamp'])
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df[df['amount'] > 0]
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        self.logger.info(f"After cleaning: {len(df):,} records")
        return df
    
    def transform_ecommerce(self, df):
        """Transform e-commerce dataset to standard schema."""
        self.logger.info("Transforming E-Commerce dataset...")
        
        df = df.copy()
        
        # Standardize each field
        df = self.standardize_user_id(df, 'order_id')
        df = self.standardize_amount(df, 'order_value')
        df = self.standardize_city(df, 'location')
        df = self.standardize_timestamp(df, 'order_date')
        
        # Select standard columns and add source
        df = df[['user_id', 'amount', 'city', 'timestamp']].copy()
        df['source_id'] = 'ecommerce'
        
        # Clean data
        df = df.dropna(subset=['user_id', 'amount', 'timestamp'])
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df[df['amount'] > 0]
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        self.logger.info(f"After cleaning: {len(df):,} records")
        return df
    
    def transform_taxi(self, df):
        """Transform taxi dataset to standard schema."""
        self.logger.info("Transforming Taxi dataset...")
        
        df = df.copy()
        
        # Standardize each field
        df = self.standardize_user_id(df, 'trip_id')
        df = self.standardize_amount(df, 'fare_amount')
        df = self.standardize_city(df, 'pickup_location')
        df = self.standardize_timestamp(df, 'trip_timestamp')
        
        # Select standard columns and add source
        df = df[['user_id', 'amount', 'city', 'timestamp']].copy()
        df['source_id'] = 'taxi'
        
        # Clean data
        df = df.dropna(subset=['user_id', 'amount', 'timestamp'])
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df[df['amount'] > 0]
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        self.logger.info(f"After cleaning: {len(df):,} records")
        return df
    
    def transform_subscription(self, df):
        """Transform subscription dataset to standard schema."""
        self.logger.info("Transforming Subscription dataset...")
        
        df = df.copy()
        
        # Standardize each field
        df = self.standardize_user_id(df, 'subscription_id')
        df = self.standardize_amount(df, 'monthly_billing')
        df = self.standardize_city(df, 'city_name')
        df = self.standardize_timestamp(df, 'created_at')
        
        # Select standard columns and add source
        df = df[['user_id', 'amount', 'city', 'timestamp']].copy()
        df['source_id'] = 'subscription'
        
        # Clean data
        df = df.dropna(subset=['user_id', 'amount', 'timestamp'])
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df[df['amount'] > 0]
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        self.logger.info(f"After cleaning: {len(df):,} records")
        return df
    
    def combine_datasets(self, *dataframes):
        """Combine multiple datasets."""
        self.logger.info(f"Combining {len(dataframes)} datasets...")
        
        combined = pd.concat(dataframes, ignore_index=True)
        
        self.logger.info(f"Combined dataset: {len(combined):,} total records")
        return combined
    
    def scale_dataset(self, df, scale_factor=1):
        """Scale dataset by sampling with replacement."""
        if scale_factor > 1:
            self.logger.info(f"Scaling dataset with factor {scale_factor}...")
            scaled = df.sample(n=int(len(df) * scale_factor), replace=True, random_state=42)
            self.logger.info(f"Scaled dataset: {len(scaled):,} records")
            return scaled
        return df
    
    def add_quality_variance(self, df):
        """
        Add intentional data quality issues for testing.
        - Missing values
        - Outliers
        - Inconsistencies
        """
        self.logger.info("Adding quality variance for realistic testing...")
        
        df = df.copy()
        np.random.seed(42)
        
        # Add random null values (5% missing data in city)
        null_indices = np.random.choice(len(df), size=int(0.05 * len(df)), replace=False)
        df.loc[null_indices, 'city'] = None
        
        # Add some outliers (high amounts - top 2%)
        outlier_indices = np.random.choice(len(df), size=int(0.02 * len(df)), replace=False)
        df.loc[outlier_indices, 'amount'] = df.loc[outlier_indices, 'amount'] * 10
        
        return df
    
    def get_statistics(self, df):
        """Compute and log dataset statistics."""
        stats = {
            'total_records': len(df),
            'null_user_ids': df['user_id'].isna().sum(),
            'null_amounts': df['amount'].isna().sum(),
            'null_cities': df['city'].isna().sum(),
            'null_timestamps': df['timestamp'].isna().sum(),
        }
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Dataset Statistics")
        self.logger.info("=" * 60)
        self.logger.info(f"Total Records: {stats['total_records']:,}")
        self.logger.info(f"Null user_ids: {stats['null_user_ids']}")
        self.logger.info(f"Null amounts: {stats['null_amounts']}")
        self.logger.info(f"Null cities: {stats['null_cities']}")
        self.logger.info(f"Null timestamps: {stats['null_timestamps']}")
        
        self.logger.info("\nRecords by Source:")
        source_counts = df['source_id'].value_counts().sort_index()
        for source, count in source_counts.items():
            self.logger.info(f"  {source}: {count:,}")
        
        self.logger.info(f"\nAmount Statistics (USD):")
        self.logger.info(f"  Min: ${df['amount'].min():.2f}")
        self.logger.info(f"  Max: ${df['amount'].max():.2f}")
        self.logger.info(f"  Mean: ${df['amount'].mean():.2f}")
        self.logger.info(f"  Median: ${df['amount'].median():.2f}")
        self.logger.info(f"  StdDev: ${df['amount'].std():.2f}")
        
        return stats
    
    def run(self, scale_output=False, add_variance=True):
        """Execute the full pipeline."""
        self.logger.info("=" * 60)
        self.logger.info("Multi-Source Data Integration Pipeline (Pandas)")
        self.logger.info("=" * 60)
        
        # Load all datasets
        transactions_df = self.load_dataset(
            f'{RAW_DATA_DIR}/transactions.csv',
            'Transactions'
        )
        
        ecommerce_df = self.load_dataset(
            f'{RAW_DATA_DIR}/multi_source/ecommerce_data.csv',
            'E-Commerce'
        )
        
        taxi_df = self.load_dataset(
            f'{RAW_DATA_DIR}/multi_source/taxi_data.csv',
            'Taxi'
        )
        
        subscription_df = self.load_dataset(
            f'{RAW_DATA_DIR}/multi_source/subscription_data.csv',
            'Subscription'
        )
        
        # Transform each dataset to standard schema
        self.logger.info("\n" + "-" * 60)
        self.logger.info("Transforming Datasets")
        self.logger.info("-" * 60)
        
        transactions_std = self.transform_transactions(transactions_df)
        ecommerce_std = self.transform_ecommerce(ecommerce_df)
        taxi_std = self.transform_taxi(taxi_df)
        subscription_std = self.transform_subscription(subscription_df)
        
        # Combine all datasets
        self.logger.info("\n" + "-" * 60)
        self.logger.info("Combining Datasets")
        self.logger.info("-" * 60)
        
        unified = self.combine_datasets(
            transactions_std,
            ecommerce_std,
            taxi_std,
            subscription_std
        )
        
        # Scale if needed
        if scale_output:
            unified = self.scale_dataset(unified, scale_factor=2)
        
        # Add quality variance
        if add_variance:
            unified = self.add_quality_variance(unified)
        
        # Get statistics
        self.get_statistics(unified)
        
        # Save output
        self.logger.info("\n" + "-" * 60)
        self.logger.info("Saving Output")
        self.logger.info("-" * 60)
        
        # Save as Parquet
        parquet_output_path = f'{OUTPUT_DIR}/combined_unified_data.parquet'
        unified.to_parquet(parquet_output_path, index=False)
        self.logger.info(f"✓ Unified data (Parquet) saved to: {parquet_output_path}")
        
        # Save as CSV
        csv_output_path = f'{OUTPUT_DIR}/combined_unified_data.csv'
        unified.to_csv(csv_output_path, index=False)
        self.logger.info(f"✓ Unified data (CSV) saved to: {csv_output_path}")
        
        # Show sample
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Sample Output Records (First 10)")
        self.logger.info("=" * 60)
        print(unified.head(10).to_string())
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Schema Info")
        self.logger.info("=" * 60)
        self.logger.info(f"Columns: {unified.columns.tolist()}")
        self.logger.info(f"\nData Types:\n{unified.dtypes}")
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Integration Complete!")
        self.logger.info("=" * 60)
        self.logger.info(f"Total records in unified dataset: {len(unified):,}")
        self.logger.info(f"Output location: {OUTPUT_DIR}")
        
        return unified


def main():
    """Main entry point."""
    try:
        pipeline = DataIntegrationPipelinePandas()
        pipeline.run(scale_output=False, add_variance=True)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()

"""
Multi-Source Data Integration Pipeline with PySpark
Ingests, cleans, transforms, and combines multiple datasets into a unified format.

Schema Mapping:
- Source A (Transactions): user_id, purchase_amount → amount, city, timestamp, source_id
- Source B (E-Commerce): order_id → user_id, order_value → amount, location → city, order_date → timestamp
- Source C (Taxi): trip_id → user_id, fare_amount → amount, pickup_location → city, trip_timestamp → timestamp
- Source D (Subscription): subscription_id → user_id, monthly_billing → amount, city_name → city, created_at → timestamp
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, coalesce, when, to_timestamp, 
    monotonically_increasing_id, lower, trim, 
    isnan, isnull, regexp_replace
)
from pyspark.sql.types import DoubleType
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
RAW_DATA_DIR = 'data/raw'
OUTPUT_DIR = 'data/unified'
NUM_PARTITIONS = 4

class DataIntegrationPipeline:
    """Multi-source data integration and standardization."""
    
    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def load_dataset(self, path, name):
        """Load a dataset and log its schema."""
        self.logger.info(f"Loading {name} from {path}")
        df = self.spark.read.csv(path, header=True, inferSchema=True)
        self.logger.info(f"{name}: {df.count()} records, {len(df.columns)} columns")
        self.logger.info(f"Columns: {df.columns}")
        return df
    
    def standardize_amount(self, df, amount_col):
        """
        Standardize amount column to numeric double.
        Handle missing/invalid values.
        """
        df = df.withColumn(
            'amount_temp',
            when(col(amount_col).isNull(), None)
            .when(col(amount_col) < 0, None)  # Remove negative amounts
            .otherwise(col(amount_col).cast(DoubleType()))
        )
        return df.drop(amount_col).withColumnRenamed('amount_temp', 'amount')
    
    def standardize_timestamp(self, df, timestamp_col):
        """
        Standardize timestamp column to ISO format.
        Handle various timestamp formats.
        """
        df = df.withColumn(
            'timestamp_temp',
            coalesce(
                to_timestamp(col(timestamp_col), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                to_timestamp(col(timestamp_col), "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(col(timestamp_col), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col(timestamp_col))
            )
        )
        return df.drop(timestamp_col).withColumnRenamed('timestamp_temp', 'timestamp')
    
    def standardize_city(self, df, city_col):
        """
        Standardize city names.
        Clean up whitespace and case.
        """
        df = df.withColumn(
            'city',
            when(col(city_col).isNull(), 'Unknown')
            .otherwise(
                trim(
                    regexp_replace(
                        lower(col(city_col)),
                        r'\s+', ' '
                    )
                )
            )
        )
        return df.drop(city_col)
    
    def standardize_user_id(self, df, user_id_col):
        """
        Standardize user_id column.
        Use existing ID or generate one.
        """
        df = df.withColumn(
            'user_id',
            when(col(user_id_col).isNull(), 
                 concat(lit("generated_"), monotonically_increasing_id()))
            .otherwise(col(user_id_col).cast('string'))
        )
        return df.drop(user_id_col)
    
    def transform_transactions(self, df):
        """Transform transactions dataset (already has correct schema)."""
        self.logger.info("Transforming Transactions dataset...")
        
        # Already has correct columns
        df = df.select(
            col('user_id'),
            col('purchase_amount').alias('amount'),
            col('city'),
            col('timestamp'),
            col('source_id')
        )
        
        # Clean data
        df = df.filter(
            (col('user_id').isNotNull()) &
            (col('amount').isNotNull()) &
            (col('amount') > 0) &
            (col('timestamp').isNotNull())
        )
        
        return df
    
    def transform_ecommerce(self, df):
        """Transform e-commerce dataset to standard schema."""
        self.logger.info("Transforming E-Commerce dataset...")
        
        # Rename and standardize columns
        df = self.standardize_user_id(df, 'order_id')
        df = self.standardize_amount(df, 'order_value')
        df = self.standardize_city(df, 'location')
        df = self.standardize_timestamp(df, 'order_date')
        
        # Add source ID
        df = df.withColumn('source_id', lit('ecommerce'))
        
        # Select standard columns
        df = df.select('user_id', 'amount', 'city', 'timestamp', 'source_id')
        
        # Clean data
        df = df.filter(
            (col('user_id').isNotNull()) &
            (col('amount').isNotNull()) &
            (col('amount') > 0) &
            (col('timestamp').isNotNull())
        )
        
        self.logger.info(f"After cleaning: {df.count()} records")
        return df
    
    def transform_taxi(self, df):
        """Transform taxi dataset to standard schema."""
        self.logger.info("Transforming Taxi dataset...")
        
        # Rename and standardize columns
        df = self.standardize_user_id(df, 'trip_id')
        df = self.standardize_amount(df, 'fare_amount')
        df = self.standardize_city(df, 'pickup_location')
        df = self.standardize_timestamp(df, 'trip_timestamp')
        
        # Add source ID
        df = df.withColumn('source_id', lit('taxi'))
        
        # Select standard columns
        df = df.select('user_id', 'amount', 'city', 'timestamp', 'source_id')
        
        # Clean data
        df = df.filter(
            (col('user_id').isNotNull()) &
            (col('amount').isNotNull()) &
            (col('amount') > 0) &
            (col('timestamp').isNotNull())
        )
        
        self.logger.info(f"After cleaning: {df.count()} records")
        return df
    
    def transform_subscription(self, df):
        """Transform subscription dataset to standard schema."""
        self.logger.info("Transforming Subscription dataset...")
        
        # Rename and standardize columns
        df = self.standardize_user_id(df, 'subscription_id')
        df = self.standardize_amount(df, 'monthly_billing')
        df = self.standardize_city(df, 'city_name')
        df = self.standardize_timestamp(df, 'created_at')
        
        # Add source ID
        df = df.withColumn('source_id', lit('subscription'))
        
        # Select standard columns
        df = df.select('user_id', 'amount', 'city', 'timestamp', 'source_id')
        
        # Clean data
        df = df.filter(
            (col('user_id').isNotNull()) &
            (col('amount').isNotNull()) &
            (col('amount') > 0) &
            (col('timestamp').isNotNull())
        )
        
        self.logger.info(f"After cleaning: {df.count()} records")
        return df
    
    def combine_datasets(self, *dataframes):
        """Combine multiple datasets."""
        self.logger.info(f"Combining {len(dataframes)} datasets...")
        
        combined = dataframes[0]
        for df in dataframes[1:]:
            combined = combined.unionByName(df)
        
        self.logger.info(f"Combined dataset: {combined.count()} total records")
        return combined
    
    def scale_dataset(self, df, scale_factor=2):
        """Scale dataset by sampling with replacement."""
        self.logger.info(f"Scaling dataset with factor {scale_factor}...")
        scaled = df.sample(withReplacement=True, fraction=scale_factor, seed=42)
        self.logger.info(f"Scaled dataset: {scaled.count()} records")
        return scaled
    
    def add_quality_variance(self, df):
        """
        Add intentional data quality issues for testing.
        - Missing values
        - Outliers
        - Inconsistencies
        """
        from pyspark.sql.functions import rand, when
        
        self.logger.info("Adding quality variance for realistic testing...")
        
        # Add random null values (5% missing data)
        df = df.withColumn(
            'city',
            when(rand() < 0.05, None).otherwise(col('city'))
        )
        
        # Add some outliers (high amounts - top 2%)
        df = df.withColumn(
            'amount',
            when(rand() < 0.02, col('amount') * 10)  # 10x multiplier
            .otherwise(col('amount'))
        )
        
        return df
    
    def get_statistics(self, df):
        """Compute and log dataset statistics."""
        stats = {
            'total_records': df.count(),
            'null_user_ids': df.filter(col('user_id').isNull()).count(),
            'null_amounts': df.filter(col('amount').isNull()).count(),
            'null_cities': df.filter(col('city').isNull()).count(),
            'null_timestamps': df.filter(col('timestamp').isNull()).count(),
        }
        
        # By source
        source_stats = df.groupBy('source_id').count().collect()
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Dataset Statistics")
        self.logger.info("=" * 60)
        self.logger.info(f"Total Records: {stats['total_records']:,}")
        self.logger.info(f"Null user_ids: {stats['null_user_ids']}")
        self.logger.info(f"Null amounts: {stats['null_amounts']}")
        self.logger.info(f"Null cities: {stats['null_cities']}")
        self.logger.info(f"Null timestamps: {stats['null_timestamps']}")
        
        self.logger.info("\nRecords by Source:")
        for row in source_stats:
            self.logger.info(f"  {row['source_id']}: {row['count']:,}")
        
        amount_stats = df.agg({
            'amount': ['min', 'max', 'avg', 'stddev']
        }).collect()[0]
        
        self.logger.info(f"\nAmount Statistics (USD):")
        self.logger.info(f"  Min: ${amount_stats[0]:.2f}")
        self.logger.info(f"  Max: ${amount_stats[1]:.2f}")
        self.logger.info(f"  Avg: ${amount_stats[2]:.2f}")
        self.logger.info(f"  StdDev: ${amount_stats[3]:.2f}")
        
        return stats
    
    def run(self, scale_output=True, add_variance=True):
        """Execute the full pipeline."""
        self.logger.info("=" * 60)
        self.logger.info("Multi-Source Data Integration Pipeline")
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
            unified = self.scale_dataset(unified, scale_factor=1)  # Change factor if needed
        
        # Add quality variance
        if add_variance:
            unified = self.add_quality_variance(unified)
        
        # Get statistics
        self.get_statistics(unified)
        
        # Save output
        self.logger.info("\n" + "-" * 60)
        self.logger.info("Saving Output")
        self.logger.info("-" * 60)
        
        output_path = f'{OUTPUT_DIR}/combined_unified_data'
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        unified.repartition(NUM_PARTITIONS).write \
            .mode('overwrite') \
            .parquet(output_path)
        
        self.logger.info(f"✓ Unified data saved to: {output_path}")
        
        # Also save as CSV for easy inspection
        csv_output_path = f'{OUTPUT_DIR}/combined_unified_data.csv'
        unified.coalesce(1).write \
            .mode('overwrite') \
            .csv(csv_output_path, header=True)
        
        self.logger.info(f"✓ CSV export saved to: {csv_output_path}")
        
        # Show sample
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Sample Output Records")
        self.logger.info("=" * 60)
        unified.limit(10).show(truncate=False)
        
        return unified


def create_spark_session():
    """Create Spark session with appropriate configuration."""
    return SparkSession.builder \
        .appName("DataIntegrationPipeline") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()


def main():
    """Main entry point."""
    try:
        spark = create_spark_session()
        pipeline = DataIntegrationPipeline(spark)
        pipeline.run(scale_output=True, add_variance=True)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == '__main__':
    main()

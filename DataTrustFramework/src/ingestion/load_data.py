from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def load_csv_data(spark, file_path: str) -> DataFrame:
    """
    Loads raw CSV dataset using Spark, infers schema, and adds an ingestion timestamp.
    """
    
    # Define an explicit schema to make parsing robust, or rely on inferSchema
    # For this project, we'll infer schema but you could also define it explicitly
    df = spark.read.csv(
        file_path, 
        header=True, 
        inferSchema=True
    )
    
    # Add ingestion timestamp column
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    return df

"""
Streaming ingestion module for Data Trust Scoring Framework.
Loads Kafka stream and parses JSON messages.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
from pyspark.sql.types import StructType
from typing import Optional


def load_kafka_stream(spark, 
                      bootstrap_servers: str = "localhost:9092",
                      topic: str = "transactions",
                      schema: Optional[StructType] = None,
                      starting_offset: str = "latest") -> DataFrame:
    """
    Load streaming data from Kafka topic.
    
    Args:
        spark: SparkSession object
        bootstrap_servers: Kafka server address
        topic: Topic to subscribe to
        schema: StructType schema for parsing JSON
        starting_offset: "latest" or "earliest"
        
    Returns:
        Streaming DataFrame
    """
    
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offset) \
        .option("failOnDataLoss", "false") \
        .load()
    
    return df


def parse_kafka_json(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Parse Kafka messages (value column contains JSON string).
    Extract JSON fields and handle parsing errors gracefully.
    
    Args:
        df: Streaming DataFrame from Kafka (has 'value' column with JSON string)
        schema: StructType schema for parsing JSON
        
    Returns:
        DataFrame with parsed fields
    """
    
    # Parse JSON from Kafka value column
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    return parsed_df


def prepare_ingestion_fields(df: DataFrame) -> DataFrame:
    """
    Prepare ingestion record and convert timestamp to proper format.
    
    Args:
        df: Parsed DataFrame from Kafka
        
    Returns:
        DataFrame with ingestion_timestamp added and timestamp parsed
    """
    
    # Convert timestamp string to TimestampType
    df = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    )
    
    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    return df


def load_and_parse_kafka(spark, 
                         bootstrap_servers: str = "localhost:9092",
                         topic: str = "transactions",
                         schema: Optional[StructType] = None,
                         starting_offset: str = "latest") -> DataFrame:
    """
    Complete ingestion pipeline: Load Kafka stream and parse JSON messages.
    
    Args:
        spark: SparkSession
        bootstrap_servers: Kafka server
        topic: Topic name
        schema: JSON schema
        starting_offset: Starting offset
        
    Returns:
        Ingested streaming DataFrame
    """
    
    # Load from Kafka
    df = load_kafka_stream(spark, bootstrap_servers, topic, schema, starting_offset)
    
    # Parse JSON
    df = parse_kafka_json(df, schema)
    
    # Prepare ingestion fields
    df = prepare_ingestion_fields(df)
    
    return df

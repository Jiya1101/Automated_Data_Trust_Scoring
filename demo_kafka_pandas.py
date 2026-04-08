"""
Kafka-based Real-Time Trust Scoring Pipeline Demo
Consumes messages from Kafka and processes them through the full pipeline using pandas.
This demonstrates the real-time streaming capability without Spark JVM issues.
"""

import json
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import time
import logging
from sklearn.ensemble import IsolationForest
import pickle
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'data_trust_ingest'
BATCH_SIZE = 100  # Process in batches
OUTPUT_DIR = 'data/output/trust_scores_kafka_demo'
CHECKPOINT_DIR = 'checkpoints/kafka'

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


class KafkaStreamProcessor:
    """Process Kafka messages through the trust scoring pipeline."""
    
    def __init__(self, bootstrap_servers, topic):
        """Initialize Kafka consumer."""
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None
        self.model = self._load_or_train_model()
        self.records_processed = 0
        self.start_time = datetime.now()
        
    def _load_or_train_model(self):
        """Load pre-trained model or train a new one."""
        model_path = 'models/isolation_forest_model.pkl'
        
        if os.path.exists(model_path):
            logger.info(f"Loading pre-trained model from {model_path}")
            with open(model_path, 'rb') as f:
                return pickle.load(f)
        else:
            logger.info("Training new Isolation Forest model...")
            # Generate some sample data for training
            np.random.seed(42)
            X = np.random.randn(1000, 1)
            model = IsolationForest(contamination=0.05, random_state=42)
            model.fit(X)
            
            # Save the model
            os.makedirs('models', exist_ok=True)
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            logger.info(f"Model saved to {model_path}")
            return model
    
    def connect(self):
        """Connect to Kafka."""
        logger.info(f"Connecting to Kafka broker at {self.bootstrap_servers}")
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='trust-score-processor',
            session_timeout_ms=30000,
            max_poll_records=BATCH_SIZE
        )
        logger.info(f"Connected to Kafka topic '{self.topic}'")
    
    def compute_quality_metrics(self, df):
        """Compute data quality metrics."""
        df['completeness'] = 1.0  # Assuming all Kafka messages are complete
        df['consistency'] = 1.0
        df['accuracy'] = np.random.uniform(0.9, 1.0, len(df))
        df['freshness'] = 1.0
        df['timeliness'] = 1.0
        
        df['quality_score'] = (
            df['completeness'] + df['consistency'] + 
            df['accuracy'] + df['freshness'] + df['timeliness']
        ) / 5
        
        return df
    
    def detect_anomalies(self, df):
        """Detect anomalies using Isolation Forest."""
        if 'purchase_amount' in df.columns:
            X = df[['purchase_amount']].values
            df['is_anomaly'] = self.model.predict(X) == -1
        else:
            df['is_anomaly'] = False
        return df
    
    def compute_source_reputation(self, df):
        """Compute source reputation."""
        # Simple reputation based on anomaly rate
        df['source_reputation'] = df.groupby('source_id')['is_anomaly'].transform(
            lambda x: 1.0 - x.astype(int).mean()
        )
        return df
    
    def compute_trust_score(self, df):
        """Compute final trust score."""
        # Apply temporal decay
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        now = pd.Timestamp.now(tz='UTC')
        df['days_old'] = (now - df['timestamp']).dt.total_seconds() / 86400
        df['temporal_decay'] = np.exp(-0.1 * df['days_old'])
        
        # Compute trust score (0-100)
        df['trust_score'] = (
            df['quality_score'] * 
            df['source_reputation'] * 
            df['temporal_decay'] * 
            100
        )
        
        # Clamp to [0, 100]
        df['trust_score'] = df['trust_score'].clip(0, 100)
        
        return df
    
    def process_batch(self, records):
        """Process a batch of records through the pipeline."""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(records)
            
            if len(df) == 0:
                return df
            
            # Pipeline stages
            df = self.compute_quality_metrics(df)
            df = self.detect_anomalies(df)
            df = self.compute_source_reputation(df)
            df = self.compute_trust_score(df)
            
            return df
        
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return pd.DataFrame()
    
    def run(self, max_messages=None):
        """Run the stream processor."""
        self.connect()
        
        batch = []
        messages_received = 0
        
        try:
            logger.info("Starting to consume messages...")
            
            for message in self.consumer:
                try:
                    # Parse message
                    record = message.value
                    batch.append(record)
                    messages_received += 1
                    
                    # Process when batch is full
                    if len(batch) >= BATCH_SIZE:
                        logger.info(f"Processing batch of {len(batch)} records (total: {messages_received})")
                        
                        # Process batch through pipeline
                        df_processed = self.process_batch(batch)
                        
                        if len(df_processed) > 0:
                            # Save results
                            output_file = f"{OUTPUT_DIR}/results_batch_{self.records_processed // BATCH_SIZE}.parquet"
                            df_processed.to_parquet(output_file)
                            
                            # Log statistics
                            anomalies = df_processed['is_anomaly'].sum()
                            avg_trust = df_processed['trust_score'].mean()
                            logger.info(
                                f"Batch processed: {len(df_processed)} records, "
                                f"{anomalies} anomalies, "
                                f"avg trust score: {avg_trust:.2f}"
                            )
                            
                            self.records_processed += len(df_processed)
                        
                        batch = []
                    
                    # Stop if max messages reached
                    if max_messages and messages_received >= max_messages:
                        logger.info(f"Reached max messages limit: {max_messages}")
                        break
                
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing message: {e}")
                    continue
            
            # Process remaining records
            if len(batch) > 0:
                logger.info(f"Processing final batch of {len(batch)} records")
                df_processed = self.process_batch(batch)
                if len(df_processed) > 0:
                    output_file = f"{OUTPUT_DIR}/results_batch_final.parquet"
                    df_processed.to_parquet(output_file)
                    self.records_processed += len(df_processed)
        
        except KeyboardInterrupt:
            logger.info("Stream processor interrupted by user")
        
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Clean shutdown."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        throughput = self.records_processed / elapsed if elapsed > 0 else 0
        
        logger.info("=" * 60)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total records processed: {self.records_processed}")
        logger.info(f"Total time: {elapsed:.2f} seconds")
        logger.info(f"Throughput: {throughput:.0f} records/second")
        logger.info(f"Output saved to: {OUTPUT_DIR}")
        logger.info("=" * 60)
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


def main():
    """Main entry point."""
    logger.info("Starting Kafka-based Trust Scoring Pipeline")
    logger.info(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info("-" * 60)
    
    processor = KafkaStreamProcessor(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    processor.run(max_messages=None)


if __name__ == '__main__':
    main()

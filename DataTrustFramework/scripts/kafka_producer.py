"""
Kafka Producer for Data Trust Scoring Framework.
Reads transaction data from CSV and streams it to Kafka topic in real-time.

Usage:
    python kafka_producer.py --csv-file data/raw/transactions.csv --topic transactions --delay 0.1
"""

import json
import csv
import time
import argparse
from datetime import datetime
from kafka import KafkaProducer
from typing import Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TransactionProducer:
    """Produces transaction records to Kafka topic."""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "transactions"):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers (host:port)
            topic: Kafka topic name
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
        )
        logger.info(f"Connected to Kafka at {bootstrap_servers}, topic: {topic}")
    
    def send_record(self, record: dict, key: Optional[str] = None) -> bool:
        """
        Send a single record to Kafka.
        
        Args:
            record: Dictionary containing transaction data
            key: Optional message key (defaults to user_id)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if key is None:
                key = record.get('user_id', 'unknown')
            
            future = self.producer.send(
                self.topic,
                value=record,
                key=key.encode('utf-8')
            )
            # Wait for record to be sent with timeout
            future.get(timeout=10)
            return True
        except Exception as e:
            logger.error(f"Failed to send record: {e}")
            return False
    
    def stream_from_csv(self, csv_file: str, start_row: int = 0, 
                       max_records: Optional[int] = None, 
                       delay: float = 0.0):
        """
        Stream transaction records from CSV file to Kafka.
        
        Args:
            csv_file: Path to CSV file
            start_row: Starting row number (for resuming)
            max_records: Maximum records to send (None = all)
            delay: Delay between records in seconds (simulates real-time)
        """
        records_sent = 0
        errors = 0
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                logger.info(f"Starting to stream from {csv_file}")
                logger.info(f"Delay between records: {delay}s")
                
                for row_idx, row in enumerate(reader):
                    if row_idx < start_row:
                        continue
                    
                    if max_records and records_sent >= max_records:
                        logger.info(f"Reached max_records limit: {max_records}")
                        break
                    
                    try:
                        # Clean and validate record
                        record = self._clean_record(row)
                        
                        # Send to Kafka
                        if self.send_record(record):
                            records_sent += 1
                            if records_sent % 100 == 0:
                                logger.info(f"Sent {records_sent} records...")
                        else:
                            errors += 1
                        
                        # Apply delay to simulate real-time
                        if delay > 0:
                            time.sleep(delay)
                    
                    except Exception as e:
                        logger.error(f"Error processing row {row_idx}: {e}")
                        errors += 1
                        continue
        
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_file}")
            return
        except KeyboardInterrupt:
            logger.info("Producer interrupted by user")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Producer shutdown. Sent: {records_sent}, Errors: {errors}")
    
    @staticmethod
    def _clean_record(row: dict) -> dict:
        """
        Clean and validate a record from CSV.
        
        Args:
            row: Raw CSV row as dictionary
            
        Returns:
            Cleaned record dictionary
        """
        record = {}
        
        # Required fields
        record['user_id'] = str(row.get('user_id', '')).strip()
        record['source_id'] = str(row.get('source_id', 'unknown')).strip()
        record['timestamp'] = str(row.get('timestamp', datetime.now().isoformat())).strip()
        
        # Optional fields
        record['city'] = str(row.get('city', '')).strip()
        
        # Parse purchase_amount
        try:
            amount_str = str(row.get('purchase_amount', '0')).strip()
            record['purchase_amount'] = float(amount_str) if amount_str else None
        except ValueError:
            logger.warning(f"Invalid purchase_amount: {row.get('purchase_amount')}")
            record['purchase_amount'] = None
        
        # Validate required fields
        if not record['user_id']:
            raise ValueError("Missing required field: user_id")
        if not record['timestamp']:
            raise ValueError("Missing required field: timestamp")
        
        return record


def main():
    parser = argparse.ArgumentParser(
        description='Stream transaction data from CSV to Kafka'
    )
    parser.add_argument(
        '--csv-file',
        type=str,
        default='DataTrustFramework/data/raw/transactions.csv',
        help='Path to CSV file containing transactions'
    )
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (host:port)'
    )
    parser.add_argument(
        '--topic',
        type=str,
        default='transactions',
        help='Kafka topic name'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.1,
        help='Delay between records in seconds (default: 0.1)'
    )
    parser.add_argument(
        '--max-records',
        type=int,
        default=None,
        help='Maximum records to send (default: all)'
    )
    parser.add_argument(
        '--start-row',
        type=int,
        default=0,
        help='Starting row (for resuming interrupted streams)'
    )
    
    args = parser.parse_args()
    
    # Create producer and start streaming
    producer = TransactionProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    producer.stream_from_csv(
        csv_file=args.csv_file,
        start_row=args.start_row,
        max_records=args.max_records,
        delay=args.delay
    )


if __name__ == '__main__':
    main()

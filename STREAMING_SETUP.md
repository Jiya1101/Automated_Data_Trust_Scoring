# 🚀 Real-Time Data Trust Scoring Pipeline (Kafka + Spark Structured Streaming)

This document provides comprehensive instructions for running the converted streaming version of the Data Trust Scoring Framework.

## 📋 Overview

The pipeline has been converted from **batch processing** to **real-time streaming** using:
- **Apache Kafka** as the message broker
- **PySpark Structured Streaming** for stream processing
- **Isolation Forest** for anomaly detection (pre-trained model)

### Architecture

```
┌──────────────────┐        ┌──────────────┐        ┌─────────────────────┐
│ Transaction CSV  │───────▶│ Kafka Topic  │───────▶│ Spark Streaming Job │
│ (Kafka Producer) │        │ "transactions"       │ (Main Pipeline)     │
└──────────────────┘        └──────────────┘        └──────────┬──────────┘
                                                                │
                                    ┌───────────────────────────┘
                                    │
                                    ▼
                            ┌─────────────────┐
                            │ Trust Scores    │
                            │ (Parquet/CSV)   │
                            └─────────────────┘
```

## 🔧 Prerequisites

### System Requirements
- Python 3.8+
- Java 11+ (for Spark)
- Docker & Docker Compose (for Kafka)
- Apache Spark 3.5.0+

### Python Dependencies
```bash
pip install pyspark>=3.5.0
pip install kafka-python>=2.0.0
pip install scikit-learn>=1.3.0
pip install pandas>=2.0.0
pip install numpy>=1.24.0
```

The dependencies are already listed in `requirements.txt`. Update it to include kafka-python:

```bash
pip install -r requirements.txt
pip install kafka-python
```

---

## 🚀 Quick Start (5 Steps)

### Step 1: Start Kafka with Docker Compose

```bash
# Navigate to project root
cd c:\jiya\big-data-scoring

# Start Kafka cluster (Zookeeper + Kafka + Kafka UI)
docker-compose up -d

# Verify services are running
docker-compose ps

# Check Kafka UI at http://localhost:8080 (optional)
```

**What this does:**
- Starts Zookeeper (Kafka coordination)
- Starts Kafka broker on localhost:9092
- Starts Kafka UI for monitoring
- All services are health-checked

---

### Step 2: Activate Python Virtual Environment

```bash
# Windows
.\venv\Scripts\Activate.ps1

# Linux/Mac
source venv/bin/activate
```

---

### Step 3: Train Initial Anomaly Detection Model

Before starting the streaming pipeline, train the Isolation Forest model on historical data:

```bash
python -c "
import os
import sys
sys.path.append('.')
from src.pipeline.spark_session import create_spark_session
from src.streaming.anomaly_detection_streaming import AnomalyDetectionModel

spark = create_spark_session()
df = spark.read.csv('DataTrustFramework/data/raw/transactions.csv', header=True, inferSchema=True)
AnomalyDetectionModel.train_model(df, sample_fraction=0.1, output_path='models/isolation_forest_model.pkl')
print('Model training complete!')
spark.stop()
"
```

Or use the helper script (if created):
```bash
python DataTrustFramework/scripts/train_model.py
```

---

### Step 4: Start the Kafka Producer (Terminal 1)

The producer reads from the CSV file and streams records to Kafka:

```bash
# Terminal 1
python DataTrustFramework/scripts/kafka_producer.py \
  --csv-file DataTrustFramework/data/raw/transactions.csv \
  --bootstrap-servers localhost:9092 \
  --topic transactions \
  --delay 0.1 \
  --max-records 1000000
```

**Parameters:**
- `--csv-file`: Path to CSV with transaction data
- `--bootstrap-servers`: Kafka server address
- `--topic`: Kafka topic name (must match consumer)
- `--delay`: Delay between records in seconds (simulates real-time)
- `--max-records`: Max number of records to send

**Output Example:**
```
2024-04-08 10:25:30 - __main__ - INFO - Connected to Kafka at localhost:9092, topic: transactions
2024-04-08 10:25:30 - __main__ - INFO - Starting to stream from DataTrustFramework/data/raw/transactions.csv
Sent 100 records...
Sent 200 records...
```

---

### Step 5: Start the Streaming Pipeline (Terminal 2)

```bash
# Terminal 2
python DataTrustFramework/src/pipeline/main_pipeline_streaming.py
```

**Pipeline Output Example:**
```
======================================================================
Starting Streaming Data Trust Scoring Pipeline
======================================================================

Phase 1: Loading Kafka stream...
Phase 2: Computing quality metrics...
Phase 3: Detecting anomalies...
Phase 4: Computing source reputation...
Phase 5: Computing final trust scores...
Phase 6: Preparing output columns...
Phase 7: Starting streaming write to parquet...

Streaming pipeline started successfully!
Streaming to: data/output/trust_scores_streaming
Checkpoint location: /tmp/trustscoring_checkpoints

======================================================================
Pipeline is running. Press Ctrl+C to stop.
======================================================================

-------------------------------------------
Batch: 0
-------------------------------------------
+-------+---------+----------+-----+-----+-----+-----+-----+-----------+----------+---+-----+-----+-----+-----------+
|user_id|source_id|purchase...|city |ts...|comp...|cons...|acc...|fres...|time...|is_...|ano...|src_...|tem...|trust_...|
+-------+---------+----------+-----+-----+-----+-----+-----+-----------+----------+---+-----+-----+-----+-----------+
|       |         |          |     |     |      |      |      |         |         |     |      |     |     |         |
+-------+---------+----------+-----+-----+-----+-----+-----+-----------+----------+---+-----+-----+-----+-----------+
```

---

## 📊 Output Files

After running, check the output directory:

```bash
# View processed data in Parquet format
ls -la data/output/trust_scores_streaming/

# Convert Parquet to CSV for inspection
python -c "
import pandas as pd
df = pd.read_parquet('data/output/trust_scores_streaming/')
df.to_csv('data/output/trust_scores_sample.csv', index=False)
print('Saved to CSV')
"
```

---

## 🔍 Monitoring & Debugging

### 1. Kafka UI Dashboard
Open http://localhost:8080 to see:
- Topics and partitions
- Consumer groups
- Message throughput
- Lag monitoring

### 2. Check Kafka Topic

```bash
# List topics
docker exec -it big-data-scoring-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# Describe topic
docker exec -it big-data-scoring-kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic transactions

# Monitor messages in real-time
docker exec -it big-data-scoring-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning --max-messages 10
```

### 3. Check Spark Streaming Status

The pipeline writes to `data/output/trust_scores_streaming/` with checkpoint data at `/tmp/trustscoring_checkpoints/`.

```bash
# List checkpoint files
ls -la /tmp/trustscoring_checkpoints/

# List output directory
ls -la data/output/trust_scores_streaming/
```

### 4. Logs

Enable detailed logging by setting environment variable:

```bash
# Windows PowerShell
$env:SPARK_LOG_LEVEL = "DEBUG"
python DataTrustFramework/src/pipeline/main_pipeline_streaming.py

# Linux/Mac
export SPARK_LOG_LEVEL=DEBUG
python DataTrustFramework/src/pipeline/main_pipeline_streaming.py
```

---

## ⚙️ Configuration

Edit `src/pipeline/main_pipeline_streaming.py` or create a config file:

```python
# In main_pipeline_streaming.py:
class StreamingPipelineConfig:
    def __init__(self):
        self.kafka_bootstrap_servers = "localhost:9092"  # Kafka server
        self.kafka_topic = "transactions"                # Topic name
        self.starting_offset = "latest"                  # or "earliest"
        
        self.output_format = "parquet"    # "console", "parquet", "csv"
        self.output_path = "data/output/trust_scores_streaming"
        self.checkpoint_dir = "/tmp/trustscoring_checkpoints"
        
        self.watermark_delay = "10 minutes"  # Allow late data
        self.trigger_interval = "5 seconds"  # Process every 5s
        
        self.reputation_window = "1 hour"    # Source reputation window
```

---

## 🧪 Testing

### Test 1: Small Sample
```bash
python DataTrustFramework/scripts/kafka_producer.py \
  --csv-file DataTrustFramework/data/raw/transactions.csv \
  --max-records 100 \
  --delay 0.5
```

### Test 2: End-to-End Flow
1. Start Kafka
2. Start Producer with small sample
3. Start Pipeline
4. Verify output files are created
5. Check trust_score values

### Test 3: Fault Tolerance
1. Stop pipeline (Ctrl+C)
2. Restart pipeline
3. Should resume from checkpoint

---

## 🛑 Stopping the Pipeline

### Method 1: Graceful Shutdown
```bash
# In the pipeline terminal
Press Ctrl+C
# Wait for graceful shutdown
```

### Method 2: Stop Kafka
```bash
docker-compose down

# Verify services are stopped
docker-compose ps
```

### Method 3: Cleanup
```bash
# Remove all containers and volumes
docker-compose down -v

# Remove checkpoint data
rm -rf /tmp/trustscoring_checkpoints/
```

---

## 🔄 Architecture Deep Dive

### Streaming DataFrame Transformation

```
Kafka Messages (JSON)
       ↓
[load_and_parse_kafka] → Parse JSON, add ingestion_timestamp
       ↓
[compute_quality_metrics_streaming] → Add: completeness, consistency, accuracy, freshness, timeliness
       ↓
[detect_anomalies_streaming] → Add: is_anomaly, anomaly_severity (using pre-trained model)
       ↓
[compute_source_reputation_streaming] → Add: source_reputation (windowed aggregation)
       ↓
[compute_trust_score_streaming] → Add: temporal_decay, trust_score (final = avg(metrics) * reputation * decay)
       ↓
[Output Sink] → Write to Parquet/CSV/Console
```

### Key Differences from Batch Pipeline

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| **Input** | CSV file | Kafka topic |
| **Processing** | All data at once | Micro-batches (5s intervals) |
| **Anomaly Model** | Train per run | Pre-trained, broadcast |
| **Source Reputation** | Global stats | Windowed (1 hour) |
| **Output** | Single file | Appended to directory |
| **Fault Tolerance** | Restart = reprocess | Checkpoint recovery |
| **Watermarking** | N/A | Handles late data (10 min) |

---

## 📝 Schema Reference

### Input Schema (from Kafka)
```json
{
  "user_id": "uuid-string",
  "purchase_amount": 123.45,
  "city": "New York",
  "timestamp": "2024-04-08T10:25:30",
  "source_id": "source1"
}
```

### Output Schema
```
user_id          | string  | User identifier
source_id        | string  | Data source identifier
timestamp        | timestamp | Record timestamp
purchase_amount  | double  | Transaction amount
is_anomaly       | boolean | Anomaly flag
anomaly_severity | double  | Anomaly score
source_reputation| double  | Source trust score (0-1)
temporal_decay   | double  | Time-based decay factor
trust_score      | double  | Final trust score (0-100)
completeness     | double  | Data completeness (0-1)
consistency      | double  | Data consistency (0-1)
accuracy         | double  | Data accuracy (0-1)
freshness        | double  | Data freshness (0-1)
timeliness       | double  | Data timeliness (0-1)
```

---

## 🐛 Troubleshooting

### Issue: "Connection refused" when connecting to Kafka
**Solution:**
```bash
# Check if Kafka is running
docker-compose ps

# If not, start it
docker-compose up -d

# Wait 30 seconds for Kafka to be ready
sleep 30
```

### Issue: "Topic auto-creation failed"
**Solution:**
```bash
# Manually create topic
docker exec big-data-scoring-kafka-1 kafka-topics \
  --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Issue: "Model not found" error
**Solution:**
```bash
# Train the model first
python -c "
import sys
sys.path.append('.')
from src.pipeline.spark_session import create_spark_session
from src.streaming.anomaly_detection_streaming import AnomalyDetectionModel

spark = create_spark_session()
df = spark.read.csv('DataTrustFramework/data/raw/transactions.csv', header=True, inferSchema=True)
AnomalyDetectionModel.train_model(df, output_path='models/isolation_forest_model.pkl')
spark.stop()
"
```

### Issue: "Out of memory" error
**Solution:**
```bash
# Increase Spark driver memory in spark_session.py
.config("spark.driver.memory", "4g")  # Increase to 4GB

# Or reduce batch processing
--delay 1.0  # Increase delay between messages
--max-records 10000  # Start with smaller dataset
```

### Issue: "Checkpoint location error"
**Solution:**
```bash
# Clear checkpoints
rm -rf /tmp/trustscoring_checkpoints/

# Restart pipeline
python DataTrustFramework/src/pipeline/main_pipeline_streaming.py
```

---

## 📚 Additional Resources

### File Structure
```
DataTrustFramework/
├── scripts/
│   ├── kafka_producer.py          # Kafka producer
│   └── generate_dataset.py        # Original batch generator
├── src/
│   ├── pipeline/
│   │   ├── spark_session.py       # Spark config
│   │   ├── main_pipeline.py       # Original batch pipeline
│   │   └── main_pipeline_streaming.py  # NEW: Streaming pipeline
│   ├── streaming/
│   │   ├── schema_definitions.py           # NEW: Schema definitions
│   │   ├── load_kafka_stream.py            # NEW: Kafka ingestion
│   │   ├── quality_metrics_streaming.py    # NEW: Quality metrics
│   │   ├── anomaly_detection_streaming.py  # NEW: Anomaly detection
│   │   ├── source_reputation_streaming.py  # NEW: Source reputation
│   │   └── trust_score_streaming.py        # NEW: Trust score
│   ├── quality_metrics/ (batch)
│   ├── anomaly_detection/ (batch)
│   ├── reputation/ (batch)
│   └── trust_score/ (batch)
├── data/
│   ├── raw/
│   │   └── transactions.csv
│   └── output/
│       └── trust_scores_streaming/  # Output directory
├── models/
│   └── isolation_forest_model.pkl   # Pre-trained model
└── docker-compose.yml               # Kafka infrastructure
```

### Key Files Changed/Added
- ✅ `src/pipeline/spark_session.py` - Added Kafka packages
- ✅ `src/pipeline/main_pipeline_streaming.py` - NEW streaming main pipeline
- ✅ `src/streaming/` - NEW module for streaming implementations
- ✅ `scripts/kafka_producer.py` - NEW Kafka producer
- ✅ `docker-compose.yml` - NEW Kafka infrastructure

---

## 🎯 Next Steps / Enhancements

1. **Stateful Streaming**: Use MapGroupsWithState for more accurate reputation tracking
2. **MongoDB Output**: Store results in MongoDB for persistence
3. **Dashboarding**: Add Grafana/Kibana for visualization
4. **Alerting**: Trigger alerts when trust_score drops below threshold
5. **Model Retraining**: Automatically retrain anomaly model every N hours
6. **Backpressure Handling**: Implement rate limiting for high-throughput scenarios
7. **Distributed Deployment**: Deploy on Kubernetes with S3/Azure Storage for checkpoints

---

## 📞 Support

For issues:
1. Check logs: `docker-compose logs kafka`
2. Review schema: `src/streaming/schema_definitions.py`
3. Test producer: `python scripts/kafka_producer.py --max-records 10`
4. Verify Kafka: `docker exec big-data-scoring-kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092`

---

**Happy Streaming! 🎉**

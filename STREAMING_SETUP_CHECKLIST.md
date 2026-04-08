# ✅ Streaming Pipeline Verification Checklist

Use this checklist to verify your streaming pipeline setup is complete and working.

---

## 📋 Pre-Setup Checklist

- [ ] Python 3.8+ installed (`python --version`)
- [ ] Java 11+ installed (`java -version`)
- [ ] Docker installed (`docker --version`)
- [ ] Docker Compose installed (`docker-compose --version`)
- [ ] Virtual environment created (`ls venv/`)
- [ ] Dependencies installed (`pip list | grep pyspark`)
- [ ] CSV data file exists (`ls DataTrustFramework/data/raw/transactions.csv`)

---

## 🔧 Files Created Checklist

### Streaming Modules
- [ ] `src/streaming/schema_definitions.py`
- [ ] `src/streaming/load_kafka_stream.py`
- [ ] `src/streaming/quality_metrics_streaming.py`
- [ ] `src/streaming/anomaly_detection_streaming.py`
- [ ] `src/streaming/source_reputation_streaming.py`
- [ ] `src/streaming/trust_score_streaming.py`

### Main Pipeline
- [ ] `src/pipeline/main_pipeline_streaming.py`

### Scripts
- [ ] `scripts/kafka_producer.py`
- [ ] `scripts/train_model.py`

### Infrastructure
- [ ] `docker-compose.yml`

### Documentation
- [ ] `STREAMING_SETUP.md`
- [ ] `STREAMING_CONVERSION_SUMMARY.md`
- [ ] `quick_start.sh`
- [ ] `quick_start.ps1`

### Modified Files
- [ ] `src/pipeline/spark_session.py` (updated with Kafka packages)
- [ ] `requirements.txt` (added kafka-python)

---

## 🚀 Environment Setup Checklist

### Step 1: Kafka Infrastructure
- [ ] `docker-compose up -d` executed successfully
- [ ] `docker-compose ps` shows all services running
- [ ] Zookeeper is healthy
- [ ] Kafka broker is healthy
- [ ] Kafka UI is accessible at http://localhost:8080

### Step 2: Python Environment
- [ ] Virtual environment activated
- [ ] Dependencies installed: `pip install -r requirements.txt`
- [ ] If error, install missing: `pip install kafka-python`

### Step 3: Model Training
- [ ] Ran: `python DataTrustFramework/scripts/train_model.py`
- [ ] Model file exists: `ls -la models/isolation_forest_model.pkl`
- [ ] Model file size > 1MB (sanity check)

### Step 4: Create Output Directories
- [ ] `mkdir -p data/output/trust_scores_streaming`
- [ ] `mkdir -p /tmp/trustscoring_checkpoints` (Linux/Mac)
- [ ] `New-Item -Path C:\tmp\trustscoring_checkpoints -ItemType Directory -Force` (Windows)

---

## 🧪 Component Testing Checklist

### Test: Kafka Producer
```bash
python DataTrustFramework/scripts/kafka_producer.py \
  --csv-file DataTrustFramework/data/raw/transactions.csv \
  --max-records 10 \
  --delay 0.5
```
- [ ] Producer starts without errors
- [ ] Output shows: "Sent X records..."
- [ ] No connection refused errors
- [ ] Producer completes successfully

### Test: Kafka Topic
```bash
# Check if topic exists
docker exec big-data-scoring-kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --list | grep transactions
```
- [ ] Topic "transactions" is listed

### Test: Spark Session
```bash
python -c "
import sys
sys.path.append('.')
from src.pipeline.spark_session import create_spark_session
spark = create_spark_session()
print('Spark version:', spark.version)
spark.stop()
"
```
- [ ] No errors
- [ ] Spark version printed
- [ ] Kafka packages loaded

### Test: Schema Definitions
```bash
python -c "
import sys
sys.path.append('.')
from src.streaming.schema_definitions import KAFKA_INPUT_SCHEMA
print('Schema fields:', [f.name for f in KAFKA_INPUT_SCHEMA.fields])
"
```
- [ ] No import errors
- [ ] Schema fields printed correctly

### Test: Anomaly Model
```bash
python -c "
import sys
sys.path.append('.')
from src.streaming.anomaly_detection_streaming import AnomalyDetectionModel
model = AnomalyDetectionModel.load_model('models/isolation_forest_model.pkl')
print('Model loaded successfully')
"
```
- [ ] No errors
- [ ] Model loaded message printed

---

## ▶️ Pipeline Execution Checklist

### Terminal 1: Start Kafka Producer
```bash
python DataTrustFramework/scripts/kafka_producer.py \
  --csv-file DataTrustFramework/data/raw/transactions.csv \
  --delay 0.1 \
  --max-records 1000
```
- [ ] Producer starts
- [ ] Shows "Connected to Kafka"
- [ ] Shows "Starting to stream"
- [ ] Message count increases: "Sent 100 records..."

### Terminal 2: Start Streaming Pipeline
```bash
python DataTrustFramework/src/pipeline/main_pipeline_streaming.py
```
- [ ] Pipeline starts
- [ ] Shows all phases: Ingestion, Quality, Anomaly, Reputation, Score
- [ ] No errors during startup
- [ ] Shows "Pipeline is running"
- [ ] Displays: "Pipeline is running. Press Ctrl+C to stop."

### Monitoring

#### Option 1: Kafka UI (Browser)
- [ ] Open http://localhost:8080
- [ ] See "transactions" topic
- [ ] Message count increases
- [ ] Lag is low (< 5 min)

#### Option 2: Check Output Files
```bash
ls -la data/output/trust_scores_streaming/
```
- [ ] Directory contains .parquet files
- [ ] Files are growing in size

#### Option 3: Read Sample Output
```bash
python -c "
import pandas as pd
df = pd.read_parquet('data/output/trust_scores_streaming')
print('Records processed:', len(df))
print('Columns:', df.columns.tolist())
print(df.head())
"
```
- [ ] Can read parquet successfully
- [ ] Records count > 0
- [ ] trust_score column present
- [ ] Data looks reasonable

---

## 🛑 Shutdown Checklist

### Normal Shutdown
- [ ] Stop Pipeline: `Ctrl+C` in Terminal 2
- [ ] Stop Producer: `Ctrl+C` in Terminal 1
- [ ] Checkpoint created: `ls -la /tmp/trustscoring_checkpoints/`

### Full Cleanup
```bash
docker-compose down
```
- [ ] All containers stopped
- [ ] No errors during shutdown

### Checkpoint Cleanup (if needed)
```bash
rm -rf /tmp/trustscoring_checkpoints/
```
- [ ] Checkpoint removed
- [ ] Next run will start fresh

---

## 🔄 Restart Checklist

After first successful run, verify recovery:

### Stop and Restart Producer
- [ ] Stop producer: `Ctrl+C`
- [ ] Restart: `python scripts/kafka_producer.py ---max-records 100 --delay 1.0`
- [ ] New records appear in Kafka

### Stop and Restart Pipeline
- [ ] Stop pipeline: `Ctrl+C` in Terminal 2
- [ ] Verify checkpoint exists: `ls -la /tmp/trustscoring_checkpoints/`
- [ ] Restart: `python src/pipeline/main_pipeline_streaming.py`
- [ ] Pipeline resumes from checkpoint (no gaps)

---

## ⚠️ Troubleshooting Checklist

### If Producer Fails: "Connection refused"
- [ ] Check Kafka is running: `docker-compose ps`
- [ ] If not: `docker-compose up -d`
- [ ] Wait 30 seconds for Kafka to be ready
- [ ] Verify topic exists: `docker exec big-data-scoring-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list`

### If Pipeline Fails: "Model not found"
- [ ] Train model: `python DataTrustFramework/scripts/train_model.py`
- [ ] Verify file exists: `ls -la models/isolation_forest_model.pkl`
- [ ] Restart pipeline

### If Pipeline Fails: "Out of memory"
- [ ] Reduce delay: `--delay 1.0` (slower stream)
- [ ] Reduce max records: `--max-records 1000`
- [ ] Clear checkpoints: `rm -rf /tmp/trustscoring_checkpoints/`

### If Output is Empty
- [ ] Check producer is running: verify "Sent X records" messages
- [ ] Check pipeline didn't error out
- [ ] Wait longer (5-10 micro-batches)
- [ ] Check output path: `ls data/output/trust_scores_streaming/`

---

## 📊 Success Criteria

Your setup is successful when:

✅ Kafka services are running and healthy
✅ Kafka UI is accessible at http://localhost:8080
✅ Producer sends records to Kafka topic
✅ Pipeline starts without errors
✅ Trust scores are computed and written to Parquet
✅ Output files exist in `data/output/trust_scores_streaming/`
✅ Can read parquet files with pandas
✅ Pipeline logs show all phases completing
✅ trust_score values are between 0 and 100
✅ is_anomaly is boolean (true/false)

---

## 🎯 Performance Baseline

Expected performance on standard hardware:

| Metric | Expected | Acceptable Range |
|--------|----------|------------------|
| Latency (per batch) | 5-10 seconds | 3-15 seconds |
| Records/sec | 100-1000 | 50-5000 |
| Producer CPU | < 20% | < 40% |
| Pipeline CPU | 30-50% | 20-80% |
| Memory usage | 2-4 GB | 1-6 GB |
| Checkpoint size | 10-50 MB | 5-100 MB |

---

## 📝 Notes

Use this space to document any custom configurations or issues encountered:

```
_____________________________________________________________________

_____________________________________________________________________

_____________________________________________________________________
```

---

## ✅ Final Verification

Before going to production, ensure:

- [ ] All tests pass
- [ ] Output data quality is acceptable
- [ ] No errors in logs
- [ ] Performance meets requirements
- [ ] Checkpoint recovery works
- [ ] Documentation understood
- [ ] Team trained on operations

---

**Status**: [ ] Setup Complete ✅ | [ ] Setup In Progress 🔄 | [ ] Setup Failed ❌

**Date**: _______________

**Verified by**: _______________

**Notes**: _______________________________________________________________

---

For detailed help, see STREAMING_SETUP.md

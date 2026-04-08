#!/bin/bash
# Quick start script for Linux/Mac
# Run the entire streaming pipeline setup in one go

set -e

echo "========================================================================"
echo "Data Trust Scoring Framework - Streaming Pipeline Quick Start"
echo "========================================================================"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Step 1: Check prerequisites
echo -e "\n${YELLOW}[Step 1]${NC} Checking prerequisites..."
command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required but not installed."; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed."; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed."; exit 1; }
echo -e "${GREEN}✓${NC} All prerequisites found"

# Step 2: Start Kafka
echo -e "\n${YELLOW}[Step 2]${NC} Starting Kafka with Docker Compose..."
docker-compose up -d
echo "Waiting for Kafka to be ready (30 seconds)..."
sleep 30
echo -e "${GREEN}✓${NC} Kafka is running"

# Step 3: Activate venv
echo -e "\n${YELLOW}[Step 3]${NC} Activating Python virtual environment..."
source venv/bin/activate
echo -e "${GREEN}✓${NC} Virtual environment activated"

# Step 4: Train model
echo -e "\n${YELLOW}[Step 4]${NC} Training anomaly detection model..."
python DataTrustFramework/scripts/train_model.py \
  --input DataTrustFramework/data/raw/transactions.csv \
  --output models/isolation_forest_model.pkl \
  --sample-fraction 0.1
echo -e "${GREEN}✓${NC} Model trained"

# Step 5: Instructions
echo -e "\n${GREEN}========================================================================"
echo "Setup Complete! 🎉"
echo "========================================================================${NC}"
echo ""
echo "Next steps:"
echo ""
echo "1. Start Kafka Producer (in Terminal 1):"
echo "   python DataTrustFramework/scripts/kafka_producer.py \\"
echo "     --csv-file DataTrustFramework/data/raw/transactions.csv \\"
echo "     --delay 0.1"
echo ""
echo "2. Start Streaming Pipeline (in Terminal 2):"
echo "   python DataTrustFramework/src/pipeline/main_pipeline_streaming.py"
echo ""
echo "3. Monitor Kafka (optional):"
echo "   Open http://localhost:8080 in your browser"
echo ""
echo "4. Stop Kafka when done:"
echo "   docker-compose down"
echo ""

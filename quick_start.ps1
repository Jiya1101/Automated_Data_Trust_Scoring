# Quick start script for Windows PowerShell
# Run the entire streaming pipeline setup in one go

Write-Host "========================================================================" -ForegroundColor Cyan
Write-Host "Data Trust Scoring Framework - Streaming Pipeline Quick Start" -ForegroundColor Cyan
Write-Host "========================================================================" -ForegroundColor Cyan

# Step 1: Check prerequisites
Write-Host "`n[Step 1] Checking prerequisites..." -ForegroundColor Yellow

# Check Python
try {
    $pythonVersion = & python --version 2>&1
    Write-Host "✓ Python found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ Python is required but not installed." -ForegroundColor Red
    exit 1
}

# Check Docker
try {
    $dockerVersion = & docker --version 2>&1
    Write-Host "✓ Docker found: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "✗ Docker is required but not installed." -ForegroundColor Red
    exit 1
}

# Step 2: Start Kafka
Write-Host "`n[Step 2] Starting Kafka with Docker Compose..." -ForegroundColor Yellow
& docker-compose up -d
Write-Host "Waiting for Kafka to be ready (30 seconds)..." -ForegroundColor White
Start-Sleep -Seconds 30
Write-Host "✓ Kafka is running" -ForegroundColor Green

# Step 3: Activate venv
Write-Host "`n[Step 3] Activating Python virtual environment..." -ForegroundColor Yellow
& .\venv\Scripts\Activate.ps1
Write-Host "✓ Virtual environment activated" -ForegroundColor Green

# Step 4: Train model
Write-Host "`n[Step 4] Training anomaly detection model..." -ForegroundColor Yellow
& python DataTrustFramework/scripts/train_model.py `
  --input DataTrustFramework/data/raw/transactions.csv `
  --output models/isolation_forest_model.pkl `
  --sample-fraction 0.1
Write-Host "✓ Model trained" -ForegroundColor Green

# Step 5: Instructions
Write-Host "`n" -ForegroundColor Green
Write-Host "========================================================================" -ForegroundColor Green
Write-Host "Setup Complete! 🎉" -ForegroundColor Green
Write-Host "========================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Start Kafka Producer (in Terminal 1):" -ForegroundColor White
Write-Host "   python DataTrustFramework/scripts/kafka_producer.py \" -ForegroundColor Gray
Write-Host "     --csv-file DataTrustFramework/data/raw/transactions.csv \" -ForegroundColor Gray
Write-Host "     --delay 0.1" -ForegroundColor Gray
Write-Host ""
Write-Host "2. Start Streaming Pipeline (in Terminal 2):" -ForegroundColor White
Write-Host "   python DataTrustFramework/src/pipeline/main_pipeline_streaming.py" -ForegroundColor Gray
Write-Host ""
Write-Host "3. Monitor Kafka (optional):" -ForegroundColor White
Write-Host "   Open http://localhost:8080 in your browser" -ForegroundColor Gray
Write-Host ""
Write-Host "4. Stop Kafka when done:" -ForegroundColor White
Write-Host "   docker-compose down" -ForegroundColor Gray
Write-Host ""
Write-Host "For detailed instructions, see STREAMING_SETUP.md" -ForegroundColor Cyan

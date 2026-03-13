# Automated Data Trust Scoring Framework for Multi-Source Big Data Pipelines

This project implements a batch-processing Spark pipeline that ingests multi-source datasets, evaluates data quality, detects anomalies, computes source reputation, applies temporal decay, and produces a final trust score for each record.

## Setup Instructions

1. **Install Prerequisites**: Ensure you have Python 3.9+ and Apache Spark 3.5+ installed on your local machine.

2. **Virtual Environment**: Create a virtual environment and install the dependencies.
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Generate Dataset**: Run the script to generate the synthetic 1-2 million records dataset.
   ```bash
   python scripts/generate_dataset.py
   ```

4. **Run the Pipeline**: Execute the main PySpark pipeline.
   ```bash
   python src/pipeline/main_pipeline.py
   ```

## Output
The final trust scores and features will be saved to `data/output/trust_scores` in Parquet format. Visualizations will be generated and saved in the project root or displayed.

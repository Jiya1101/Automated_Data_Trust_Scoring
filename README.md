# Automated Data Trust Scoring Framework

A scalable Big Data pipeline that evaluates the reliability of large-scale transactional datasets using distributed processing and machine learning.

## Overview

In modern data systems, data often comes from multiple sources and may contain inconsistencies, missing values, or anomalous patterns. This project builds a **Data Trust Scoring Framework** that evaluates the reliability of each data record and assigns a **trust score between 0 and 100**.

The framework uses **Apache Spark for distributed processing** and **Isolation Forest for anomaly detection**, enabling scalable data quality evaluation.

---

## Architecture

Dataset Generation  
↓  
PySpark Data Pipeline  
↓  
Data Quality Metrics  
↓  
Anomaly Detection (Isolation Forest)  
↓  
Source Reputation Calculation  
↓  
Trust Score Computation  
↓  
Parquet Storage  
↓  
Visualization

---

## Technologies Used

- **PySpark** – Distributed data processing
- **Scikit-learn** – Isolation Forest anomaly detection
- **Pandas** – Data analysis
- **Faker** – Synthetic dataset generation
- **Matplotlib / Seaborn** – Data visualization
- **Parquet** – Efficient columnar storage format

---

## Dataset

The project generates **~1 million synthetic transaction records** to simulate real-world data pipelines.

Fields include:

- transaction_id
- user_id
- source_id
- amount
- timestamp
- location
- device

---

## Pipeline Phases

### 1. Data Ingestion
Loads raw CSV data into Spark DataFrames.

### 2. Data Quality Metrics
Evaluates completeness, validity, and consistency of records.

### 3. Anomaly Detection
Uses **Isolation Forest** to identify unusual records.

### 4. Source Reputation
Measures reliability of each data source based on anomaly frequency.

### 5. Trust Score Calculation
Combines quality metrics, anomaly results, and source reputation.

### 6. Storage
Processed data stored in **Parquet format**.

### 7. Visualization
Generates charts showing trust score distributions and anomaly patterns.

---

## How to Run

### 1. Install dependencies

```bash
pip install -r DataTrustFramework/requirements.txt
2. Generate dataset
python DataTrustFramework/scripts/generate_dataset.py
3. Run pipeline
python DataTrustFramework/src/pipeline/main_pipeline.py
4. Generate visualizations
python DataTrustFramework/src/visualization/plots.py




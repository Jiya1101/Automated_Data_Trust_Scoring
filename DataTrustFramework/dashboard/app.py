import os
import sys
# Add the parent directory (DataTrustFramework) to system path so 'src' can be found
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename

app = Flask(__name__)
CORS(app)

# Settings
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

from src.pipeline.spark_session import create_spark_session
from src.quality_metrics.compute_metrics import compute_quality_metrics
from src.anomaly_detection.isolation_forest import detect_anomalies
from src.reputation.source_reputation import compute_source_reputation
from src.trust_score.trust_score import compute_trust_score
from pyspark.sql.functions import current_timestamp
import json

# Global pure JVM allocation for repeated lightning-fast analysis queries
spark = create_spark_session("TrustLensDashboard")

def process_in_spark(filepath):
    try:
        # Load directly over the JVM bypassing Pandas OOM limits
        df = spark.read.csv(filepath, header=True, inferSchema=True, nullValue="NULL")
        
        # Add fundamental requirements if missing
        lower_cols = [c.lower() for c in df.columns]
        
        if "ingestion_timestamp" not in lower_cols:
            from pyspark.sql.functions import current_timestamp
            df = df.withColumn("ingestion_timestamp", current_timestamp())
            lower_cols.append("ingestion_timestamp")
            
        if "source_id" not in lower_cols:
            from pyspark.sql.functions import lit
            df = df.withColumn("source_id", lit("user_upload"))
            lower_cols.append("source_id")
        
        # Robustly ensure 'amount' exists without causing case-sensitive Ambiguous Reference
        if "amount" not in lower_cols:
            # Avoid renaming 'id' or 'index' columns to 'amount' which create fake perfect uniform distributions
            numeric_cols = [c for c, t in df.dtypes if (t.startswith('int') or t.startswith('double') or t.startswith('float')) and c.lower() not in ['id', 'index', 'idx']]
            if numeric_cols:
                # rename the first valid numeric column to 'amount' cleanly
                df = df.withColumnRenamed(numeric_cols[0], "amount")
            else:
                from pyspark.sql.functions import lit
                from pyspark.sql.types import DoubleType
                # Use -999.0 instead of 0.0 so that Scikit-Learn doesn't crash on NaN,
                # BUT the PySpark accuracy evaluations (0 to 100,000 bounds) will correctly result in 0% Accuracy!
                df = df.withColumn("amount", lit(-999.0).cast(DoubleType()))
        else:
            # If it exists but is capitalized (e.g. 'Amount'), rename it strictly to lowercase 'amount'
            original_amount_col = [c for c in df.columns if c.lower() == "amount"][0]
            if original_amount_col != "amount":
                df = df.withColumnRenamed(original_amount_col, "amount")
                
        # Also ensure 'user_id', 'city', 'timestamp' for compute_quality_metrics
        # IMPORTANT: Use safe dummies so datasets lacking explicit timestamps (like creditcard.csv) do not get 0% Freshness and Timeliness automatically
        from pyspark.sql.types import StringType, TimestampType
        from pyspark.sql.functions import current_timestamp
        
        if "user_id" not in lower_cols:
            df = df.withColumn("user_id", lit("unknown").cast(StringType()))
        if "city" not in lower_cols:
            df = df.withColumn("city", lit("unknown").cast(StringType()))
        if "timestamp" not in lower_cols:
            df = df.withColumn("timestamp", current_timestamp())

        # Sub-sample heavily for immediate API response speed (prevents taking too long natively)
        sample_size = min(10000, df.count())
        if sample_size > 0:
            df = df.sample(fraction=min(1.0, 10000.0 / sample_size), seed=42).limit(5000)

        # PySpark Transformer Execution
        df = compute_quality_metrics(df)
        df = detect_anomalies(df, sample_fraction=0.1) # Isolation Forest ML phase
        df = compute_source_reputation(df)
        df = compute_trust_score(df)
        
        # JVM Cache optimization for the downstream native calculations
        df.cache()
        total_records = df.count()
        if total_records == 0:
            return None
        
        # Calculate scalar averages strictly via JVM execution DAG
        from pyspark.sql import functions as F
        aggs = df.agg(
            F.avg("completeness").alias("completeness"),
            F.avg("accuracy").alias("accuracy"),
            F.avg("source_reputation").alias("reputation"),
            F.avg("trust_score").alias("trust_score"),
            (F.sum(F.when(F.col("is_anomaly") == True, 1).otherwise(0)) / total_records).alias("anomaly_rate")
        ).collect()[0]

        # Heavy extraction: Top 10 High-Risk Anomalies dynamically collected
        top_10 = []
        if "anomaly_severity" in df.columns:
            top_anomalies_df = df.filter(F.col("is_anomaly") == True)\
                .orderBy(F.col("anomaly_severity").desc())\
                .limit(10).toPandas()
                
            # Filter just the critical readable columns for UI table
            cols_to_show = [c for c in ["user_id", "source_id", "amount", "trust_score", "anomaly_severity"] if c in top_anomalies_df.columns]
            top_10 = top_anomalies_df[cols_to_show].to_dict(orient="records")

        metrics = {
            "total_records": total_records,
            "completeness": round(float(aggs["completeness"] or 0) * 100, 2),
            "accuracy": round(float(aggs["accuracy"] or 0) * 100, 2),
            "anomaly_rate": round(float(aggs["anomaly_rate"] or 0) * 100, 2),
            "reputation": round(float(aggs["reputation"] or 0) * 100, 2),
            "trust_score": round(float(aggs["trust_score"] or 0), 2),
            "top_10_anomalies": top_10
        }
        return {"status": "success", "metrics": metrics}
    except Exception as e:
        return {"status": "error", "message": f"PySpark Execution Failed: {str(e)}"}

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400
        
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "No selected file"}), 400
        
    if file and file.filename.endswith('.csv'):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Native PySpark Processing triggering Spark DAG implicitly avoiding OOM!
        results = process_in_spark(filepath)
        
        if results is None:
            return jsonify({"status": "error", "message": "Empty CSV uploaded"}), 400
            
        return jsonify(results)
            
    return jsonify({"status": "error", "message": "File type not supported"}), 400


if __name__ == '__main__':
    app.run(debug=True, port=5000)

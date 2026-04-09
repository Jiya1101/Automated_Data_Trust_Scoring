import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import BooleanType, DoubleType
from sklearn.ensemble import IsolationForest

def detect_anomalies(df: DataFrame, sample_fraction: float = 0.1) -> DataFrame:
    """
    Trains an Isolation Forest model on a sample of the data and applies it
    using a Pandas UDF for scalable anomaly detection.

    Works with the UNIFIED schema column 'amount' (renamed from 'purchase_amount').
    """

    # 1. Take a sample for training
    print(f"Sampling {sample_fraction * 100}% of data for Isolation Forest training...")
    sample_df = (
        df.select("amount")
          .sample(fraction=sample_fraction, seed=42)
          .toPandas()
    )

    # Handle NaNs for training
    sample_df["amount"] = sample_df["amount"].fillna(sample_df["amount"].median())

    # 2. Train Isolation Forest
    print("Training Isolation Forest on sampled dataset...")
    clf = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    clf.fit(sample_df[["amount"]])

    # 3. Pandas UDFs — capture clf in closure

    @pandas_udf(BooleanType())
    def predict_anomaly(amount_series: pd.Series) -> pd.Series:
        imputed = amount_series.fillna(0.0).to_frame(name="amount")
        preds = clf.predict(imputed)
        return pd.Series(preds == -1)   # True = anomaly

    @pandas_udf(DoubleType())
    def predict_severity(amount_series: pd.Series) -> pd.Series:
        imputed = amount_series.fillna(0.0).to_frame(name="amount")
        scores = clf.decision_function(imputed)
        return pd.Series(-1.0 * scores)  # higher = more anomalous

    print("Applying predictions via Pandas UDF...")

    # 4. Apply to the full DataFrame using 'amount'
    result_df = (
        df
        .withColumn("is_anomaly",       predict_anomaly(col("amount")))
        .withColumn("anomaly_severity",  predict_severity(col("amount")))
    )

    return result_df

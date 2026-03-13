import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StructType, StructField, BooleanType, DoubleType
from sklearn.ensemble import IsolationForest

def detect_anomalies(df: DataFrame, sample_fraction: float = 0.1) -> DataFrame:
    """
    Trains an Isolation Forest model on a sample of the data and applies it
    using a Pandas UDF for scalable anomaly detection.
    """
    
    # 1. Take a sample for training to avoid overwhelming the driver memory
    # We only need the purchase_amount for training
    print(f"Sampling {sample_fraction * 100}% of data for Isolation Forest training...")
    sample_df = df.select("purchase_amount").sample(fraction=sample_fraction, seed=42).toPandas()
    
    # Handle NaNs for training
    sample_df['purchase_amount'] = sample_df['purchase_amount'].fillna(sample_df['purchase_amount'].median())
    
    # 2. Train Isolation Forest
    print("Training Isolation Forest on sampled dataset...")
    # contamination="auto" is common but we can set 0.05 based on our data generation logic
    clf = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
    clf.fit(sample_df[['purchase_amount']])
    
    # 3. Define Pandas UDF to predict using the trained model
    # We broadcast the model implicitly by using it in the UDF closure. Keep it light.
    
    # Define the return schema for the UDF
    # We will return a struct containing both flag and score, or we can just make two UDFs.
    # Two UDFs might be simpler to write, but let's make two UDFs since Pandas UDF with structs 
    # requires a specific return dataframe format.
    
    @pandas_udf(BooleanType())
    def predict_anomaly(purchase_series: pd.Series) -> pd.Series:
        # Fill NaN with 0 or mean to prevent sklearn error
        imputed_series = purchase_series.fillna(0.0).to_frame(name='purchase_amount')
        # clf.predict returns 1 for inliers, -1 for outliers
        preds = clf.predict(imputed_series)
        # Convert to boolean: True if anomaly (-1), False otherwise (1)
        return pd.Series(preds == -1)
        
    @pandas_udf(DoubleType())
    def predict_severity(purchase_series: pd.Series) -> pd.Series:
        imputed_series = purchase_series.fillna(0.0).to_frame(name='purchase_amount')
        # clf.decision_function returns anomaly score. Lower -> more anomalous.
        # We want to invert this so that higher means more severe.
        scores = clf.decision_function(imputed_series)
        # Invert score so positive means anomalous, negative means normal
        inverted_scores = -1.0 * scores
        return pd.Series(inverted_scores)
        
    print("Applying predictions via Pandas UDF...")
    
    # 4. Apply UDFs to the PySpark DataFrame
    result_df = df.withColumn("is_anomaly", predict_anomaly(col("purchase_amount"))) \
                  .withColumn("anomaly_severity", predict_severity(col("purchase_amount")))
                  
    return result_df

"""
Streaming Anomaly Detection module for Data Trust Scoring Framework.
Uses a pre-trained Isolation Forest model for real-time anomaly detection.

NOTE: In streaming, we cannot continuously retrain the model on every micro-batch.
Instead, we:
1. Train the model once on historical data
2. Persist the model to disk/cloud storage
3. Load and broadcast the model for streaming prediction
4. Optionally retrain periodically (e.g., daily) with accumulated data
"""

import pandas as pd
import pickle
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, pandas_udf, lit
from pyspark.sql.types import BooleanType, DoubleType
from sklearn.ensemble import IsolationForest
import logging

logger = logging.getLogger(__name__)


class AnomalyDetectionModel:
    """Manages the Isolation Forest model for anomaly detection."""
    
    MODEL_PATH = "models/isolation_forest_model.pkl"
    
    @staticmethod
    def train_model(df: DataFrame, 
                   sample_fraction: float = 0.1,
                   contamination: float = 0.05,
                   output_path: str = "models/isolation_forest_model.pkl") -> 'IsolationForest':
        """
        Train Isolation Forest model on DataFrame sample.
        
        Args:
            df: DataFrame to train on (batch or streaming)
            sample_fraction: Fraction of data to use for training
            contamination: Expected contamination ratio
            output_path: Where to save the model
            
        Returns:
            Trained IsolationForest model
        """
        logger.info(f"Training Isolation Forest with {sample_fraction*100}% sample...")
        
        # Sample data
        sample_df = df.select("purchase_amount").sample(
            fraction=sample_fraction, seed=42
        ).toPandas()
        
        # Handle NaNs
        sample_df['purchase_amount'] = sample_df['purchase_amount'].fillna(
            sample_df['purchase_amount'].median()
        )
        
        # Train model
        logger.info("Fitting Isolation Forest...")
        model = IsolationForest(
            n_estimators=100,
            contamination=contamination,
            random_state=42
        )
        model.fit(sample_df[['purchase_amount']])
        
        # Save model
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'wb') as f:
            pickle.dump(model, f)
        logger.info(f"Model saved to {output_path}")
        
        return model
    
    @staticmethod
    def load_model(model_path: str = MODEL_PATH) -> 'IsolationForest':
        """
        Load pre-trained Isolation Forest model from disk.
        
        Args:
            model_path: Path to saved model
            
        Returns:
            Loaded IsolationForest model
        """
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found at {model_path}. Train model first.")
        
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        logger.info(f"Model loaded from {model_path}")
        return model


def detect_anomalies_streaming(df: DataFrame, 
                               model_path: str = "models/isolation_forest_model.pkl") -> DataFrame:
    """
    Apply pre-trained Isolation Forest model to streaming DataFrame.
    
    Uses Pandas UDF for efficient vectorized prediction across partitions.
    
    Args:
        df: Streaming DataFrame
        model_path: Path to pre-trained model
        
    Returns:
        DataFrame with is_anomaly and anomaly_severity columns
    """
    
    logger.info(f"Loading model from {model_path}...")
    
    # Load model (this will be broadcast to workers)
    # Note: We need to be careful with model serialization across Spark workers
    try:
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        raise
    
    # Define Pandas UDFs for prediction
    @pandas_udf(BooleanType())
    def predict_anomaly(purchase_series: pd.Series) -> pd.Series:
        """Predict if records are anomalies."""
        imputed_series = purchase_series.fillna(0.0).to_frame(name='purchase_amount')
        # clf.predict returns 1 for inliers, -1 for outliers
        preds = model.predict(imputed_series)
        return pd.Series(preds == -1)
    
    @pandas_udf(DoubleType())
    def predict_severity(purchase_series: pd.Series) -> pd.Series:
        """Predict anomaly severity score."""
        imputed_series = purchase_series.fillna(0.0).to_frame(name='purchase_amount')
        # clf.decision_function returns anomaly score. Lower -> more anomalous.
        scores = model.decision_function(imputed_series)
        # Invert score so positive means anomalous
        inverted_scores = -1.0 * scores
        return pd.Series(inverted_scores)
    
    # Apply UDFs
    logger.info("Applying Isolation Forest predictions...")
    result_df = df.withColumn("is_anomaly", predict_anomaly(col("purchase_amount"))) \
                  .withColumn("anomaly_severity", predict_severity(col("purchase_amount")))
    
    return result_df


def train_and_save_model(df: DataFrame, output_path: str = "models/isolation_forest_model.pkl"):
    """
    Helper function to train and save initial model.
    Call this once with historical data before starting streaming pipeline.
    
    Args:
        df: Batch DataFrame for initial training
        output_path: Where to save the model
    """
    model = AnomalyDetectionModel.train_model(
        df, 
        sample_fraction=0.1,
        output_path=output_path
    )
    logger.info(f"Model trained and saved to {output_path}")
    return model

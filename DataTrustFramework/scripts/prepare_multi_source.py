import os
import sys

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, monotonically_increasing_id, to_timestamp, coalesce

from src.pipeline.spark_session import create_spark_session

def main():
    spark = create_spark_session(app_name="PrepareMultiSourceData")

    # 1️⃣ Load Real Datasets
    base_path = "DataTrustFramework/data/raw/multi_source/"

    df_transactions = spark.read.csv(base_path + "transactions.csv", header=True, inferSchema=True)
    df_ecommerce = spark.read.csv(base_path + "ecommerce_data.csv", header=True, inferSchema=True)
    df_subscription = spark.read.csv(base_path + "subscription_data.csv", header=True, inferSchema=True)
    df_taxi = spark.read.csv(base_path + "taxi_data.csv", header=True, inferSchema=True)

    # 2️⃣ Transform Each Dataset
    
    # 🔹 Transactions Dataset
    df_transactions = df_transactions \
        .withColumnRenamed("purchase_amount", "amount") \
        .withColumn("source_id", lit("transactions"))

    # 🔹 E-commerce Dataset
    # Actual columns: order_id, customer_name, order_value, location, order_date
    df_ecommerce = df_ecommerce \
        .withColumnRenamed("order_value", "amount") \
        .withColumnRenamed("order_date", "timestamp") \
        .withColumn("source_id", lit("ecommerce"))
    
    if "location" in df_ecommerce.columns:
        df_ecommerce = df_ecommerce.withColumnRenamed("location", "city")
        df_ecommerce = df_ecommerce.withColumn("city", coalesce(col("city"), lit("unknown")))
    else:
        df_ecommerce = df_ecommerce.withColumn("city", lit("unknown"))
        

    # 🔹 Subscription Dataset
    # Actual columns: subscription_id, user_uuid, monthly_billing, city_name, created_at
    df_subscription = df_subscription \
        .withColumnRenamed("monthly_billing", "amount") \
        .withColumnRenamed("created_at", "timestamp") \
        .withColumn("source_id", lit("subscription"))
        
    if "city_name" in df_subscription.columns:
        df_subscription = df_subscription.withColumnRenamed("city_name", "city")
        df_subscription = df_subscription.withColumn("city", coalesce(col("city"), lit("unknown")))
    else:
        df_subscription = df_subscription.withColumn("city", lit("unknown"))


    # 🔹 Taxi Dataset
    # Actual columns: trip_id, passenger_id, fare_amount, pickup_location, trip_timestamp
    df_taxi = df_taxi \
        .withColumnRenamed("fare_amount", "amount") \
        .withColumn("city", lit("NYC")) \
        .withColumnRenamed("trip_timestamp", "timestamp") \
        .withColumn("source_id", lit("taxi"))


    # 3️⃣ Handle Missing user_id
    if "user_id" not in df_transactions.columns:
        df_transactions = df_transactions.withColumn("user_id", monotonically_increasing_id())
        
    # E-commerce doesn't have a direct user_id, use monotonically_increasing_id
    df_ecommerce = df_ecommerce.withColumn("user_id", monotonically_increasing_id())

    # Subscription has user_uuid
    df_subscription = df_subscription.withColumnRenamed("user_uuid", "user_id")
    
    # Taxi has passenger_id
    df_taxi = df_taxi.withColumnRenamed("passenger_id", "user_id")


    # 4️⃣ Standardize Columns
    target_columns = ["user_id", "amount", "city", "timestamp", "source_id"]
    
    df_transactions = df_transactions.select(*target_columns).dropna()
    df_ecommerce = df_ecommerce.select(*target_columns).dropna()
    df_subscription = df_subscription.select(*target_columns).dropna()
    df_taxi = df_taxi.select(*target_columns).dropna()

    # Cast timestamp
    df_transactions = df_transactions.withColumn("timestamp", to_timestamp(col("timestamp")))
    df_ecommerce = df_ecommerce.withColumn("timestamp", to_timestamp(col("timestamp")))
    df_subscription = df_subscription.withColumn("timestamp", to_timestamp(col("timestamp")))
    df_taxi = df_taxi.withColumn("timestamp", to_timestamp(col("timestamp")))


    # 5️⃣ Instead of unionByName, append them sequentially to parquet!
    output_path = "DataTrustFramework/data/unified/combined_real_data"
    print(f"Saving data sequentially to {output_path}...")
    
    # Write first one with overwrite to initialize folder
    df_transactions.write.mode("overwrite").parquet(output_path)
    print("Transactions saved.")
    
    # Appends
    df_ecommerce.write.mode("append").parquet(output_path)
    print("Ecommerce saved.")
    
    df_subscription.write.mode("append").parquet(output_path)
    print("Subscriptions saved.")
    
    df_taxi.write.mode("append").parquet(output_path)
    print("Taxi saved.")
    
    # 6️⃣ Validate combined result
    df_combined = spark.read.parquet(output_path)
    print("Schema:")
    df_combined.printSchema()
    print("Total Records:", df_combined.count())
    print("Done!")

if __name__ == "__main__":
    main()

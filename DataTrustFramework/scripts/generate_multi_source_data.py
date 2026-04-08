"""
Multi-Source Dataset Generator
Generates diverse datasets with different schemas to simulate real-world data sources.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import os

# Random seed for reproducibility
np.random.seed(42)

OUTPUT_DIR = 'data/raw/multi_source'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configuration
NUM_RECORDS_PER_SOURCE = 50000

def generate_ecommerce_data(num_records=NUM_RECORDS_PER_SOURCE):
    """
    Generate e-commerce dataset with different schema:
    order_id | customer_name | order_value | location | order_date
    """
    print(f"Generating E-Commerce dataset ({num_records} records)...")
    
    locations = [
        'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
        'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
        'Seattle', 'Boston', 'Miami', 'Denver', 'Portland'
    ]
    
    first_names = ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana', 'Eve', 'Frank']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Lee', 'Davis']
    
    data = {
        'order_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'customer_name': [f"{np.random.choice(first_names)} {np.random.choice(last_names)}" 
                         for _ in range(num_records)],
        'order_value': np.random.uniform(10, 500, num_records),
        'location': np.random.choice(locations, num_records),
        'order_date': [
            (datetime.now() - timedelta(days=np.random.randint(0, 365))).isoformat()
            for _ in range(num_records)
        ]
    }
    
    # Add some missing values (data quality imperfections)
    df = pd.DataFrame(data)
    missing_indices = np.random.choice(len(df), size=int(0.05 * len(df)), replace=False)
    df.loc[missing_indices, 'location'] = None
    
    return df

def generate_taxi_data(num_records=NUM_RECORDS_PER_SOURCE):
    """
    Generate taxi/ride-sharing dataset with different schema:
    trip_id | passenger_id | fare_amount | pickup_location | trip_timestamp
    """
    print(f"Generating Taxi dataset ({num_records} records)...")
    
    zones = [
        'Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island',
        'Downtown', 'Midtown', 'Uptown', 'Westside', 'Eastside'
    ]
    
    data = {
        'trip_id': [str(uuid.uuid4()) for _ in range(num_records)],
        'passenger_id': [str(uuid.uuid4())[:8] for _ in range(num_records)],
        'fare_amount': np.random.uniform(5, 100, num_records),
        'pickup_location': np.random.choice(zones, num_records),
        'trip_timestamp': [
            (datetime.now() - timedelta(minutes=np.random.randint(0, 10000))).isoformat()
            for _ in range(num_records)
        ]
    }
    
    # Add some anomalies (negative fares simulating errors)
    df = pd.DataFrame(data)
    anomaly_indices = np.random.choice(len(df), size=int(0.02 * len(df)), replace=False)
    df.loc[anomaly_indices, 'fare_amount'] = -abs(df.loc[anomaly_indices, 'fare_amount'])
    
    return df

def generate_subscription_data(num_records=NUM_RECORDS_PER_SOURCE):
    """
    Generate subscription/SaaS dataset with different schema:
    subscription_id | user_uuid | monthly_billing | city_name | created_at
    """
    print(f"Generating Subscription dataset ({num_records} records)...")
    
    cities = [
        'San Francisco', 'New York', 'London', 'Tokyo', 'Berlin',
        'Paris', 'Toronto', 'Sydney', 'Singapore', 'Amsterdam'
    ]
    
    data = {
        'subscription_id': [f"SUB_{uuid.uuid4().hex[:12].upper()}" for _ in range(num_records)],
        'user_uuid': [str(uuid.uuid4()) for _ in range(num_records)],
        'monthly_billing': np.random.uniform(9.99, 299.99, num_records),
        'city_name': np.random.choice(cities, num_records),
        'created_at': [
            (datetime.now() - timedelta(days=np.random.randint(0, 730))).strftime('%Y-%m-%d %H:%M:%S')
            for _ in range(num_records)
        ]
    }
    
    return pd.DataFrame(data)

def main():
    """Generate all datasets."""
    print("=" * 60)
    print("Multi-Source Dataset Generator")
    print("=" * 60)
    
    # Generate datasets
    ecommerce_df = generate_ecommerce_data()
    taxi_df = generate_taxi_data()
    subscription_df = generate_subscription_data()
    
    # Save datasets
    ecommerce_path = f'{OUTPUT_DIR}/ecommerce_data.csv'
    taxi_path = f'{OUTPUT_DIR}/taxi_data.csv'
    subscription_path = f'{OUTPUT_DIR}/subscription_data.csv'
    
    ecommerce_df.to_csv(ecommerce_path, index=False)
    taxi_df.to_csv(taxi_path, index=False)
    subscription_df.to_csv(subscription_path, index=False)
    
    print(f"\n✓ E-Commerce data saved: {ecommerce_path} ({len(ecommerce_df)} rows)")
    print(f"✓ Taxi data saved: {taxi_path} ({len(taxi_df)} rows)")
    print(f"✓ Subscription data saved: {subscription_path} ({len(subscription_df)} rows)")
    
    # Print schema info
    print("\n" + "=" * 60)
    print("Dataset Schemas")
    print("=" * 60)
    
    print("\nE-Commerce Data:")
    print(ecommerce_df.head(2).to_string())
    print(f"Columns: {ecommerce_df.columns.tolist()}")
    
    print("\n\nTaxi Data:")
    print(taxi_df.head(2).to_string())
    print(f"Columns: {taxi_df.columns.tolist()}")
    
    print("\n\nSubscription Data:")
    print(subscription_df.head(2).to_string())
    print(f"Columns: {subscription_df.columns.tolist()}")
    
    print("\n" + "=" * 60)
    print("Generation Complete!")
    print("=" * 60)

if __name__ == '__main__':
    main()

import os
import random
import csv
from datetime import datetime, timedelta
from faker import Faker

def generate_dataset(num_records=1_000_000, output_file='data/raw/transactions.csv'):
    fake = Faker()
    sources = ['source1', 'source2', 'source3']
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    print(f"Generating {num_records} records to {output_file}...")
    
    # Calculate some timestamps for freshness/timeliness within the last 30 days
    base_date = datetime.now()
    
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['user_id', 'purchase_amount', 'city', 'timestamp', 'source_id'])
        
        for i in range(num_records):
            # Generate valid data first
            user_id = fake.uuid4()
            purchase_amount = round(random.uniform(5.0, 500.0), 2)
            city = fake.city()
            
            # Timestamp up to 30 days ago
            days_ago = random.uniform(0, 30)
            timestamp = (base_date - timedelta(days=days_ago)).isoformat()
            source_id = random.choice(sources)
            
            # Introduce anomalies (~5% of the time)
            if random.random() < 0.05:
                anomaly_type = random.choice(['missing', 'negative', 'extreme'])
                if anomaly_type == 'missing':
                    purchase_amount = ''
                    # Also randomly maybe drop city
                    if random.random() < 0.5:
                        city = ''
                elif anomaly_type == 'negative':
                    purchase_amount = -1 * round(random.uniform(10.0, 100.0), 2)
                elif anomaly_type == 'extreme':
                    purchase_amount = round(random.uniform(10_000.0, 100_000.0), 2)
                    
            writer.writerow([user_id, purchase_amount, city, timestamp, source_id])
            
            # Introduce duplicates (~1% of the time)
            if random.random() < 0.01:
                writer.writerow([user_id, purchase_amount, city, timestamp, source_id])
                
            if (i + 1) % 100_000 == 0:
                print(f"Generated {i + 1} records...")

    print("Dataset generation complete.")

if __name__ == '__main__':
    # Default to 1M records
    generate_dataset(num_records=1_000_000, output_file='data/raw/transactions.csv')

import pandas as pd
import os
import glob

def main():
    print("Preparing multi-source data using Pandas (Bypassing Windows Hadoop IO issues)...")
    
    # Paths
    base_raw = "DataTrustFramework/data/raw/multi_source"
    output_dir = "DataTrustFramework/data/unified/combined_real_data"
    os.makedirs(output_dir, exist_ok=True)
    
    dfs = []
    
    for f in glob.glob(f"{base_raw}/*.csv"):
        print(f"Loading {f}...")
        df = pd.read_csv(f)
        
        # Lowercase all columns
        df.columns = [c.lower() for c in df.columns]
        
        # Identify columns dynamically
        if "ecommerce" in f:
            df = df.rename(columns={"customer_id": "user_id", "purchase_amount": "amount", "purchase_time": "timestamp", "location": "city"})
            df["source_id"] = "ecommerce"
        elif "subscription" in f:
            df = df.rename(columns={"subscriber_id": "user_id", "monthly_fee": "amount", "signup_date": "timestamp"})
            df["source_id"] = "subscriptions"
            df["city"] = "unknown"
        elif "taxi" in f:
            df = df.rename(columns={"fare_amount": "amount", "pickup_time": "timestamp", "pickup_location": "city"})
            df["source_id"] = "taxi"
            if "user_id" not in df.columns:
                df["user_id"] = ["T" + str(i) for i in range(len(df))]
        elif "transactions" in f:
            df = df.rename(columns={"transaction_amount": "amount", "transaction_time": "timestamp", "purchase_amount": "amount"})
            df["source_id"] = "transactions"
            if "city" not in df.columns:
                df["city"] = "unknown"
        else:
            df["source_id"] = "unknown"
            
        
        # Keep only required columns, fill if missing
        cols = ["user_id", "amount", "city", "timestamp", "source_id"]
        for c in cols:
            if c not in df.columns:
                df[c] = f"missing_{c}"
        
        df = df[cols]
        dfs.append(df)

    print("Combining all data...")
    df_combined = pd.concat(dfs, ignore_index=True)
    
    print("Converting timestamp...")
    df_combined["timestamp"] = pd.to_datetime(df_combined["timestamp"], errors='coerce')
    
    # Filter out NaNs if any critical
    df_combined = df_combined.dropna(subset=["user_id", "amount", "timestamp"])

    out_file = os.path.join(output_dir, "data.parquet")
    print(f"Saving to {out_file}...")
    df_combined.to_parquet(out_file, engine="pyarrow", index=False)
    print("Done! Total records:", len(df_combined))

if __name__ == "__main__":
    main()

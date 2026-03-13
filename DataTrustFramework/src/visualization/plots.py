import os
import matplotlib.pyplot as plt
import pandas as pd

def generate_visualizations(pdf: pd.DataFrame, output_dir: str = 'plots'):
    """
    Generates and saves the required plots based on the pandas DataFrame.
    """
    os.makedirs(output_dir, exist_ok=True)
    print("Generating visualizations...")
    
    # 1. Trust Score Distribution
    plt.figure(figsize=(10, 6))
    plt.hist(pdf['trust_score'], bins=50, color='skyblue', edgecolor='black')
    plt.title('Distribution of Data Trust Scores')
    plt.xlabel('Trust Score (0-100)')
    plt.ylabel('Frequency')
    plt.grid(axis='y', alpha=0.75)
    plt.savefig(os.path.join(output_dir, 'trust_score_distribution.png'))
    plt.close()
    
    # Group by source for next two plots
    source_stats = pdf.groupby('source_id').agg(
        anomaly_count=('is_anomaly', 'sum'),
        avg_reputation=('source_reputation', 'mean')
    ).reset_index()
    
    # 2. Anomaly Count by Source
    plt.figure(figsize=(8, 5))
    plt.bar(source_stats['source_id'], source_stats['anomaly_count'], color='salmon')
    plt.title('Total Anomalies Detected by Source')
    plt.xlabel('Source ID')
    plt.ylabel('Anomaly Count')
    plt.savefig(os.path.join(output_dir, 'anomaly_count_by_source.png'))
    plt.close()
    
    # 3. Source Reputation Comparison
    plt.figure(figsize=(8, 5))
    plt.bar(source_stats['source_id'], source_stats['avg_reputation'], color='lightgreen')
    plt.title('Source Reputation Comparison')
    plt.xlabel('Source ID')
    plt.ylabel('Reputation Score (0-1)')
    plt.ylim(0, 1.1)
    for i, v in enumerate(source_stats['avg_reputation']):
        plt.text(i, v + 0.02, str(round(v, 3)), ha='center')
    plt.savefig(os.path.join(output_dir, 'source_reputation_comparison.png'))
    plt.close()
    
    print(f"Visualizations saved to {output_dir}/ directory.")

if __name__ == '__main__':
    import os
    import sys
    
    # We will read the processed data directly using Pandas because PySpark
    # initialization is currently broken on this machine due to Python 3.13 
    # native-hadoop Py4J incompatibilities that persist even in the 3.11 venv.
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    
    parquet_data_path =  r"C:\jiya\big-data-scoring\data\output\trust_scores"
    plots_output_dir = os.path.join(project_root, 'plots')
    
    print(f"Loading data from {parquet_data_path} using Pandas...")
    try:
        # Load the Parquet directory output by the pipeline
        df = pd.read_parquet(parquet_data_path)
        
        # Sample for plotting to avoid Memory issues if dataset is huge
        sample_size = min(100_000, len(df))
        if len(df) > sample_size:
            pdf = df.sample(n=sample_size, random_state=42)
        else:
            pdf = df
            
        print("Generating visualizations...")
        generate_visualizations(pdf, plots_output_dir)
        print("Done!")
    except Exception as e:
        print(f"Error reading/processing data: {e}")

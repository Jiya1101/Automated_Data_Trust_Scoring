import os
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def generate_visualizations(pdf: pd.DataFrame, output_dir: str = 'plots'):
    """
    Generates and saves a combined comprehensive interactive dashboard visualization 
    for Trust Scores, Anomalies, and Reputation strictly using Plotly.
    """
    os.makedirs(output_dir, exist_ok=True)
    print("Generating comprehensive interactive visualization...")
    
    # 1. Prepare Aggregated Metric matrix for Radar & Bar charts
    # We use completeness, accuracy, freshness (assume 1.0 if missing), reputation
    if 'freshness' not in pdf.columns:
        pdf['freshness'] = 1.0
        
    source_stats = pdf.groupby('source_id').agg(
        avg_completeness=('completeness', 'mean'),
        avg_accuracy=('accuracy', 'mean'),
        avg_freshness=('freshness', 'mean'),
        avg_reputation=('source_reputation', 'mean'),
        anomaly_count=('is_anomaly', 'sum'),
        avg_trust=('trust_score', 'mean')
    ).reset_index()

    # 2. Build Multi-Subplot layout
    fig = make_subplots(
        rows=1, cols=2, 
        specs=[[{"type": "polar"}, {"type": "xy"}]],
        subplot_titles=("Quality Matrix (Radar)", "Comprehensive Source Comparison")
    )
    
    # Trace 1: Radar Charts dynamically added for each source
    metrics_cats = ['completeness', 'accuracy', 'freshness', 'source_reputation']
    
    for i, row in source_stats.iterrows():
        # Scale to 100 for visual uniformity on radar
        r_vals = [
            row['avg_completeness'] * 100, 
            row['avg_accuracy'] * 100, 
            row['avg_freshness'] * 100, 
            row['avg_reputation'] * 100
        ]
        # Close the loop
        r_vals.append(r_vals[0])
        theta_cats = ['Completeness', 'Accuracy', 'Freshness', 'Reputation', 'Completeness']
        
        fig.add_trace(go.Scatterpolar(
            r=r_vals,
            theta=theta_cats,
            fill='toself',
            name=row['source_id'],
            opacity=0.6
        ), row=1, col=1)

    # Trace 2: Clustered Bar Source Comparison (all remaining core variables natively)
    fig.add_trace(go.Bar(
        x=source_stats['source_id'],
        y=source_stats['avg_trust'],
        name='Avg Trust Score (%)',
        marker_color='#10b981'
    ), row=1, col=2)
    
    fig.add_trace(go.Bar(
        x=source_stats['source_id'],
        y=source_stats['anomaly_count'],
        name='Total Anomalies Found',
        marker_color='#ef4444'
    ), row=1, col=2)
    
    # Formatting
    fig.update_layout(
        title_text="Data Trust Framework: Interactive Analysis Engine",
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100])
        ),
        font=dict(family="Playfair Display, serif"),
        barmode='group',
        template="plotly_dark"
    )
    
    # Save purely as HTML for full Plotly interactivity capabilities
    output_path = os.path.join(output_dir, 'comprehensive_analysis.html')
    fig.write_html(output_path)
    
    print(f"Visualization saved to {output_path}")

if __name__ == '__main__':
    import os
    import sys
    
    parquet_data_path = "data/output/trust_scores"
    plots_output_dir = "DataTrustFramework/plots"
    
    print(f"Loading data from {parquet_data_path} using Pandas...")
    try:
        df = pd.read_parquet(parquet_data_path)
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

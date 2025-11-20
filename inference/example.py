#!/usr/bin/env python3
"""
Example: Running Inference

Simple example showing how to use the inference pipeline programmatically.
"""

from pathlib import Path
import polars as pl
from inference.predict import run_inference, load_model_artifacts

def main():
    # Example 1: Run inference on vectorized data
    print("Example: Running inference on vectorized data...")
    
    model_dir = Path("model_training/models")
    vectorized_file = Path("feature_engineering/vectorized/vectorized.parquet")
    output_dir = Path("inference/results")
    
    if not vectorized_file.exists():
        print(f"Error: Vectorized file not found at {vectorized_file}")
        print("Please run the vectorization step first or use a different input.")
        return
    
    # Run inference
    results = run_inference(
        vectorized_file,
        model_dir,
        output_dir,
        include_attribution=True,
        top_n_features=5
    )
    
    # Analyze results
    print("\n=== Results Summary ===")
    print(f"Total cycles: {len(results):,}")
    
    anomalies = results.filter(pl.col("is_anomaly"))
    print(f"Anomalies detected: {len(anomalies):,} ({len(anomalies)/len(results)*100:.2f}%)")
    
    if len(anomalies) > 0:
        print(f"\nAnomaly score statistics:")
        print(f"  Min: {anomalies['anomaly_score'].min():.4f}")
        print(f"  Max: {anomalies['anomaly_score'].max():.4f}")
        print(f"  Mean: {anomalies['anomaly_score'].mean():.4f}")
        print(f"  Median: {anomalies['anomaly_score'].median():.4f}")
        
        # Show top anomalies by score
        print(f"\nTop 10 anomalies by score:")
        top_anomalies = anomalies.sort("anomaly_score").head(10)
        for row in top_anomalies.iter_rows(named=True):
            print(f"  Sequence {row['sequence_id']:.0f}, Cycle {row['cycle_id']:.0f}: "
                  f"score={row['anomaly_score']:.4f}, "
                  f"top_feature={row.get('top_feature_1', 'N/A')}")
    
    # Example 2: Load model and inspect
    print("\n=== Model Information ===")
    model, scaler, feature_cols, encoders, metadata = load_model_artifacts(model_dir)
    print(f"Model type: {type(model).__name__}")
    print(f"Number of features: {len(feature_cols)}")
    print(f"Number of encoders: {len(encoders)}")
    if metadata:
        print(f"Model metadata: {metadata.get('model_type', 'N/A')}")
        print(f"Trained at: {metadata.get('trained_at', 'N/A')}")

if __name__ == "__main__":
    main()


#!/usr/bin/env python3
"""
Inference Pipeline - Raw Data to Predictions

Complete pipeline from raw data files to anomaly predictions.
Supports starting from different stages:
- Raw data (requires parsing)
- Parsed/synced data (skips parsing)
- Cleaned data (skips parsing and cleaning)
- Vectorized data (skips all preprocessing)

Usage:
    # From raw data
    python inference/predict.py --raw-data raw_data/2025-09-02.WistDB --raw-data raw_data/2025-09-02.OPCB
    
    # From parsed data
    python inference/predict.py --parsed-data data_ingest/parsed_files/synced/2025-09-02.parquet
    
    # From cleaned data
    python inference/predict.py --cleaned-data data_ingest/data_cleaning/cleaned_data/2025-09-02.parquet
    
    # From vectorized data
    python inference/predict.py --vectorized-data feature_engineering/vectorized/vectorized.parquet
"""

import polars as pl
import numpy as np
import argparse
import logging
import json
import sys
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import joblib
import subprocess

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, LabelEncoder

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from model_training.train import prepare_features, predict_anomalies
from model_training.feature_attribution import calculate_feature_attribution
from feature_engineering.vectorize import vectorize_cycles
from data_ingest.data_cleaning.cleaning import clean_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_model_artifacts(model_dir: Path) -> Tuple[IsolationForest, StandardScaler, List[str], Dict, Dict]:
    """
    Load trained model, scaler, feature columns, encoders, and metadata.
    
    Args:
        model_dir: Directory containing model artifacts
        
    Returns:
        model: Trained Isolation Forest model
        scaler: Fitted StandardScaler
        feature_cols: List of feature column names
        encoders: Dictionary of label encoders (may be empty if not saved)
        metadata: Model metadata
    """
    logger.info(f"Loading model artifacts from {model_dir}")
    
    # Load model
    model_path = model_dir / "isolation_forest.joblib"
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")
    model = joblib.load(model_path)
    logger.info(f"Loaded model from {model_path}")
    
    # Load scaler
    scaler_path = model_dir / "scaler.joblib"
    if not scaler_path.exists():
        raise FileNotFoundError(f"Scaler file not found: {scaler_path}")
    scaler = joblib.load(scaler_path)
    logger.info(f"Loaded scaler from {scaler_path}")
    
    # Load feature columns
    feature_cols_path = model_dir / "feature_columns.json"
    if not feature_cols_path.exists():
        raise FileNotFoundError(f"Feature columns file not found: {feature_cols_path}")
    with open(feature_cols_path, 'r') as f:
        feature_cols = json.load(f)
    logger.info(f"Loaded {len(feature_cols)} feature columns")
    
    # Load encoders (optional - may not exist)
    encoders = {}
    encoders_path = model_dir / "label_encoders.joblib"
    if encoders_path.exists():
        encoders = joblib.load(encoders_path)
        logger.info(f"Loaded {len(encoders)} label encoders")
    else:
        logger.warning(f"Label encoders not found at {encoders_path}. Will create new encoders if needed.")
    
    # Load metadata (optional)
    metadata = {}
    metadata_path = model_dir / "model_metadata.json"
    if metadata_path.exists():
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        logger.info(f"Loaded model metadata")
    
    return model, scaler, feature_cols, encoders, metadata


def parse_raw_data(wistdb_file: Path, opcb_file: Path, output_dir: Path) -> Path:
    """
    Parse raw data files using the data ingestion pipeline.
    
    Args:
        wistdb_file: Path to .WistDB file
        opcb_file: Path to .OPCB file
        output_dir: Directory to save parsed/synced data
        
    Returns:
        Path to synced parquet file
    """
    logger.info("Parsing raw data files...")
    
    # Extract date from filename
    date_str = wistdb_file.stem
    
    # Run the main data ingestion script for this date
    # This will parse all files and create synced parquet
    main_script = Path(__file__).parent.parent / "data_ingest" / "main.py"
    
    logger.info(f"Running data ingestion for date: {date_str}")
    result = subprocess.run(
        [sys.executable, str(main_script), "--date", date_str],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"Data ingestion failed: {result.stderr}")
    
    # Check for synced output
    synced_file = Path(__file__).parent.parent / "data_ingest" / "parsed_files" / "synced" / f"{date_str}.parquet"
    if not synced_file.exists():
        raise FileNotFoundError(f"Synced file not created: {synced_file}")
    
    logger.info(f"Parsed data saved to {synced_file}")
    return synced_file


def clean_parsed_data(parsed_file: Path, output_dir: Path) -> Path:
    """
    Clean parsed data using the cleaning pipeline.
    
    Args:
        parsed_file: Path to parsed/synced parquet file
        output_dir: Directory to save cleaned data
        
    Returns:
        Path to cleaned parquet file
    """
    logger.info("Cleaning parsed data...")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Extract date from filename if possible, otherwise use timestamp
    if parsed_file.stem.isdigit() or len(parsed_file.stem) == 10:
        output_file = output_dir / f"{parsed_file.stem}.parquet"
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"cleaned_{timestamp}.parquet"
    
    # Clean data (function handles loading and saving)
    cleaned_df = clean_data(parsed_file, output_file, collect=True)
    logger.info(f"Cleaned data saved to {output_file}")
    
    return output_file


def vectorize_cleaned_data(cleaned_file: Path, output_dir: Path) -> Path:
    """
    Vectorize cleaned data into per-cycle features.
    
    Args:
        cleaned_file: Path to cleaned parquet file
        output_dir: Directory to save vectorized data
        
    Returns:
        Path to vectorized parquet file
    """
    logger.info("Vectorizing cleaned data...")
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Vectorize data
    vectorized_df = vectorize_cycles(cleaned_file)
    logger.info(f"Vectorized data: {len(vectorized_df):,} cycles, {len(vectorized_df.columns)} features")
    
    # Save vectorized data
    # Extract date from filename if possible
    if cleaned_file.stem.isdigit() or len(cleaned_file.stem) == 10:
        output_file = output_dir / f"vectorized_{cleaned_file.stem}.parquet"
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"vectorized_{timestamp}.parquet"
    
    vectorized_df.write_parquet(output_file)
    logger.info(f"Vectorized data saved to {output_file}")
    
    return output_file


def run_inference(
    vectorized_file: Path,
    model_dir: Path,
    output_dir: Path,
    include_attribution: bool = True,
    top_n_features: int = 5
) -> pl.DataFrame:
    """
    Run inference on vectorized data.
    
    Args:
        vectorized_file: Path to vectorized parquet file
        model_dir: Directory containing model artifacts
        output_dir: Directory to save predictions
        include_attribution: Whether to include feature attribution
        top_n_features: Number of top features to include in attribution
        
    Returns:
        DataFrame with predictions and metadata
    """
    logger.info("Running inference...")
    
    # Load model artifacts
    model, scaler, feature_cols, encoders, metadata = load_model_artifacts(model_dir)
    
    # Load vectorized data
    logger.info(f"Loading vectorized data from {vectorized_file}")
    df = pl.read_parquet(vectorized_file)
    logger.info(f"Loaded {len(df):,} cycles")
    
    # Prepare features (same as training)
    logger.info("Preparing features...")
    X, metadata_df, encoders_used = prepare_features(df, encoders=encoders)
    
    # Verify feature columns match model expectations
    # The prepare_features function filters and normalizes features
    # We need to ensure the number of features matches
    if X.shape[1] != len(feature_cols):
        logger.warning(
            f"Feature count mismatch: data has {X.shape[1]} features, "
            f"model expects {len(feature_cols)} features. "
            f"This may cause issues with feature attribution."
        )
        # For predictions, we'll proceed anyway (model may still work if columns align)
        # But we'll use only the first N features for attribution if there's a mismatch
        if X.shape[1] < len(feature_cols):
            logger.error("Data has fewer features than model expects. Cannot proceed.")
            raise ValueError(f"Feature count mismatch: {X.shape[1]} < {len(feature_cols)}")
        # If data has more features, we'll use only the first len(feature_cols)
        elif X.shape[1] > len(feature_cols):
            logger.warning(f"Data has more features than model. Using first {len(feature_cols)} features.")
            X = X[:, :len(feature_cols)]
    
    # Make predictions
    logger.info("Making predictions...")
    predictions, scores = predict_anomalies(model, scaler, X)
    
    # Convert predictions to boolean (True = anomaly)
    anomaly_mask = predictions == -1
    
    # Create results dataframe
    results = metadata_df.with_columns([
        pl.Series("anomaly_score", scores),
        pl.Series("is_anomaly", anomaly_mask),
        pl.Series("prediction", predictions)
    ])
    
    # Add feature attribution if requested
    if include_attribution:
        logger.info("Calculating feature attribution...")
        
        # Calculate feature attribution
        # X is already normalized per sequence by prepare_features
        feature_deviations, top_features = calculate_feature_attribution(
            X, anomaly_mask, feature_cols, scaler, top_n=top_n_features
        )
        
        # Add top features and deviations to results
        for i in range(top_n_features):
            col_name = f"top_feature_{i+1}"
            results = results.with_columns([
                pl.Series(col_name, [feats[i] if i < len(feats) else "" for feats in top_features])
            ])
            
            col_name = f"top_feature_{i+1}_deviation"
            deviations = []
            for j in range(len(results)):
                if i < len(top_features[j]):
                    feat_name = top_features[j][i]
                    feat_idx = feature_cols.index(feat_name) if feat_name in feature_cols else -1
                    if feat_idx >= 0:
                        deviations.append(float(feature_deviations[j, feat_idx]))
                    else:
                        deviations.append(0.0)
                else:
                    deviations.append(0.0)
            results = results.with_columns([pl.Series(col_name, deviations)])
    
    logger.info(f"Predictions complete: {np.sum(anomaly_mask):,} anomalies detected ({np.sum(anomaly_mask)/len(X)*100:.2f}%)")
    
    # Save results
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"predictions_{timestamp}.parquet"
    results.write_parquet(output_file)
    logger.info(f"Predictions saved to {output_file}")
    
    # Also save as CSV for easier inspection
    csv_file = output_dir / f"predictions_{timestamp}.csv"
    results.write_csv(csv_file)
    logger.info(f"Predictions saved to {csv_file}")
    
    return results


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description='Run inference pipeline from raw data to predictions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From raw data files
  python inference/predict.py --raw-wistdb raw_data/2025-09-02.WistDB --raw-opcb raw_data/2025-09-02.OPCB
  
  # From parsed/synced data
  python inference/predict.py --parsed-data data_ingest/parsed_files/synced/2025-09-02.parquet
  
  # From cleaned data
  python inference/predict.py --cleaned-data data_ingest/data_cleaning/cleaned_data/2025-09-02.parquet
  
  # From vectorized data
  python inference/predict.py --vectorized-data feature_engineering/vectorized/vectorized.parquet
        """
    )
    
    # Input options (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        '--raw-wistdb',
        type=Path,
        help='Path to raw .WistDB file (requires --raw-opcb)'
    )
    input_group.add_argument(
        '--parsed-data',
        type=Path,
        help='Path to parsed/synced parquet file'
    )
    input_group.add_argument(
        '--cleaned-data',
        type=Path,
        help='Path to cleaned parquet file'
    )
    input_group.add_argument(
        '--vectorized-data',
        type=Path,
        help='Path to vectorized parquet file'
    )
    
    parser.add_argument(
        '--raw-opcb',
        type=Path,
        help='Path to raw .OPCB file (required if --raw-wistdb is used)'
    )
    
    parser.add_argument(
        '--model-dir',
        type=Path,
        default=Path("model_training/models"),
        help='Directory containing model artifacts (default: model_training/models)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path("inference/results"),
        help='Directory to save predictions (default: inference/results)'
    )
    
    parser.add_argument(
        '--no-attribution',
        action='store_true',
        help='Skip feature attribution (faster but less detailed)'
    )
    
    parser.add_argument(
        '--top-n-features',
        type=int,
        default=5,
        help='Number of top features to include in attribution (default: 5)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate arguments
    if args.raw_wistdb and not args.raw_opcb:
        parser.error("--raw-opcb is required when using --raw-wistdb")
    
    try:
        # Determine starting point and run pipeline
        if args.raw_wistdb:
            # Start from raw data
            logger.info("Starting from raw data files...")
            
            # Parse raw data
            parsed_file = parse_raw_data(args.raw_wistdb, args.raw_opcb, 
                                       Path("data_ingest/parsed_files/synced"))
            
            # Clean data
            cleaned_file = clean_parsed_data(parsed_file, 
                                           Path("inference/temp/cleaned"))
            
            # Vectorize data
            vectorized_file = vectorize_cleaned_data(cleaned_file, 
                                                    Path("inference/temp/vectorized"))
            
            # Run inference
            results = run_inference(
                vectorized_file,
                args.model_dir,
                args.output_dir,
                include_attribution=not args.no_attribution,
                top_n_features=args.top_n_features
            )
            
        elif args.parsed_data:
            # Start from parsed data
            logger.info("Starting from parsed/synced data...")
            
            # Clean data
            cleaned_file = clean_parsed_data(args.parsed_data, 
                                           Path("inference/temp/cleaned"))
            
            # Vectorize data
            vectorized_file = vectorize_cleaned_data(cleaned_file, 
                                                    Path("inference/temp/vectorized"))
            
            # Run inference
            results = run_inference(
                vectorized_file,
                args.model_dir,
                args.output_dir,
                include_attribution=not args.no_attribution,
                top_n_features=args.top_n_features
            )
            
        elif args.cleaned_data:
            # Start from cleaned data
            logger.info("Starting from cleaned data...")
            
            # Vectorize data
            vectorized_file = vectorize_cleaned_data(args.cleaned_data, 
                                                    Path("inference/temp/vectorized"))
            
            # Run inference
            results = run_inference(
                vectorized_file,
                args.model_dir,
                args.output_dir,
                include_attribution=not args.no_attribution,
                top_n_features=args.top_n_features
            )
            
        elif args.vectorized_data:
            # Start from vectorized data
            logger.info("Starting from vectorized data...")
            
            # Run inference directly
            results = run_inference(
                args.vectorized_data,
                args.model_dir,
                args.output_dir,
                include_attribution=not args.no_attribution,
                top_n_features=args.top_n_features
            )
        
        # Print summary
        print("\n=== Inference Summary ===")
        print(f"Total cycles: {len(results):,}")
        n_anomalies = results["is_anomaly"].sum()
        print(f"Anomalies detected: {n_anomalies:,} ({n_anomalies/len(results)*100:.2f}%)")
        print(f"Sequences: {results['sequence_id'].n_unique()}")
        print(f"Results saved to: {args.output_dir}")
        
        if n_anomalies > 0:
            print(f"\nAnomaly score range: {results['anomaly_score'].min():.4f} to {results['anomaly_score'].max():.4f}")
            print(f"Mean anomaly score: {results['anomaly_score'].mean():.4f}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit(main())


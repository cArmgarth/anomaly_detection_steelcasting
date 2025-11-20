#!/usr/bin/env python3
"""
Train Anomaly Detection Model

Trains unsupervised anomaly detection models on vectorized casting data.
Detects anomalies and classifies anomaly types.
"""

import polars as pl
import numpy as np
import argparse
import logging
from pathlib import Path
from typing import Tuple, Dict, Optional
import json
from datetime import datetime
import joblib

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import warnings
import sys
from pathlib import Path
warnings.filterwarnings('ignore')

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_vectorized_data(input_path: Path) -> pl.DataFrame:
    """Load vectorized data from parquet file."""
    logger.info(f"Loading vectorized data from {input_path}")
    df = pl.read_parquet(input_path)
    logger.info(f"Loaded {len(df):,} cycles, {len(df.columns)} features")
    return df


def prepare_features(df: pl.DataFrame, encoders: Optional[Dict] = None) -> Tuple[np.ndarray, pl.DataFrame, Dict]:
    """
    Prepare features for model training.
    
    Separates feature columns from metadata columns.
    Encodes categorical features (casting_program, slabs_grade) using label encoding.
    Includes cast_length_m as a numeric feature.
    
    IMPORTANT: Normalizes features per sequence to focus on patterns within sequences
    rather than absolute values between sequences. This makes the model insensitive to
    overall temperature level differences between sequences.
    
    Args:
        df: Input DataFrame
        encoders: Optional dictionary of pre-fitted label encoders (for test set)
    
    Returns:
        X: Feature matrix (numpy array) - features normalized per sequence
        metadata: DataFrame with sequence_id, cycle_id, cycle_datetime, etc.
        encoders: Dictionary of label encoders for categorical features
    """
    logger.info("Preparing features...")
    
    # Columns to exclude from features (metadata/tracking only)
    exclude_cols = [
        "sequence_id", "cycle_id", "cycle_datetime"
    ]
    
    # Categorical features that need encoding
    categorical_features = ["casting_program", "slabs_grade"]
    
    # Get feature columns (all except metadata)
    all_feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    # Filter to only numeric columns and categorical features we'll encode
    # This excludes any JSON/string columns that aren't in our categorical list
    numeric_dtypes = [pl.Float64, pl.Float32, pl.Int64, pl.Int32]
    feature_cols = []
    non_numeric_cols = []
    
    for col in all_feature_cols:
        dtype = df[col].dtype
        if dtype in numeric_dtypes:
            feature_cols.append(col)
        elif col in categorical_features:
            # Include categorical features - we'll encode them
            feature_cols.append(col)
        else:
            # Exclude non-numeric columns that aren't in our categorical list
            non_numeric_cols.append(col)
    
    if non_numeric_cols:
        logger.warning(f"Excluding {len(non_numeric_cols)} non-numeric columns: {non_numeric_cols[:5]}{'...' if len(non_numeric_cols) > 5 else ''}")
    
    logger.info(f"Using {len(feature_cols)} feature columns (including casting_program, slabs_grade, cast_length_m)")
    
    # Prepare feature DataFrame
    feature_df = df.select(feature_cols)
    
    # Encode categorical features
    if encoders is None:
        encoders = {}
    
    for cat_col in categorical_features:
        if cat_col in feature_df.columns:
            # Check if column is string/categorical type
            dtype = feature_df[cat_col].dtype
            if dtype in [pl.Utf8, pl.Categorical]:
                if cat_col in encoders:
                    # Use existing encoder (for test set)
                    logger.info(f"Applying existing encoder to categorical feature: {cat_col}")
                    encoder = encoders[cat_col]
                    # Handle unseen values by encoding them as -1 or using the most common class
                    try:
                        encoded_values = encoder.transform(feature_df[cat_col].to_list())
                    except ValueError:
                        # Handle unseen values - encode as -1
                        logger.warning(f"Found unseen values in {cat_col}, encoding as -1")
                        unique_vals = set(encoder.classes_)
                        encoded_values = [
                            encoder.transform([val])[0] if val in unique_vals else -1
                            for val in feature_df[cat_col].to_list()
                        ]
                    feature_df = feature_df.with_columns([
                        pl.Series(cat_col, encoded_values, dtype=pl.Int64)
                    ])
                else:
                    # Create new encoder (for training set)
                    logger.info(f"Creating encoder for categorical feature: {cat_col}")
                    unique_vals = feature_df[cat_col].unique().to_list()
                    encoder = LabelEncoder()
                    encoder.fit(unique_vals)
                    encoders[cat_col] = encoder
                    
                    # Encode the column
                    encoded_values = encoder.transform(feature_df[cat_col].to_list())
                    feature_df = feature_df.with_columns([
                        pl.Series(cat_col, encoded_values, dtype=pl.Int64)
                    ])
                    logger.info(f"  Encoded {cat_col}: {len(unique_vals)} unique values")
    
    # Ensure all columns are numeric before converting to numpy
    # Check for any remaining non-numeric columns
    final_cols = []
    for col in feature_df.columns:
        dtype = feature_df[col].dtype
        if dtype in numeric_dtypes:
            final_cols.append(col)
        else:
            logger.warning(f"Dropping non-numeric column {col} (dtype: {dtype})")
    
    feature_df = feature_df.select(final_cols)
    
    # Normalize features per sequence to focus on patterns within sequences
    # rather than absolute values between sequences
    logger.info("Normalizing features per sequence...")
    
    # Add sequence_id back for grouping (we need it for per-sequence normalization)
    feature_df_with_seq = feature_df.with_columns(df.select("sequence_id"))
    
    # Normalize each numeric feature column per sequence
    # This removes absolute level differences between sequences
    # Formula: (value - sequence_mean) / sequence_std
    normalized_exprs = []
    for col in final_cols:
        if col in feature_df_with_seq.columns:
            # Use window/over to calculate per-sequence stats efficiently
            seq_mean = pl.col(col).mean().over("sequence_id")
            seq_std = pl.col(col).std().over("sequence_id")
            
            # Normalize: (value - sequence_mean) / (sequence_std + epsilon)
            normalized_expr = (pl.col(col) - seq_mean) / (seq_std + 1e-6)
            normalized_exprs.append(normalized_expr.alias(col))
    
    # Apply normalization
    if normalized_exprs:
        feature_df = feature_df_with_seq.select([
            *normalized_exprs,
            pl.col("sequence_id")  # Keep for now, will drop before numpy conversion
        ])
        logger.info(f"Normalized {len(normalized_exprs)} features per sequence")
    
    # Remove sequence_id before converting to numpy
    if "sequence_id" in feature_df.columns:
        feature_df = feature_df.drop("sequence_id")
    
    # Extract features as numpy array
    X = feature_df.to_numpy()
    
    # Extract metadata (include cast_length_m for evaluation)
    metadata_cols = [col for col in exclude_cols if col in df.columns]
    # Also include cast_length_m in metadata if it exists (for evaluation)
    if "cast_length_m" in df.columns and "cast_length_m" not in metadata_cols:
        metadata_cols.append("cast_length_m")
    metadata = df.select(metadata_cols)
    
    logger.info(f"Feature matrix shape: {X.shape}")
    logger.info(f"Metadata columns: {metadata_cols}")
    
    return X, metadata, encoders


def train_isolation_forest(
    X: np.ndarray,
    contamination: float = 0.1,
    n_estimators: int = 100,
    random_state: int = 42
) -> Tuple[IsolationForest, StandardScaler]:
    """
    Train Isolation Forest anomaly detection model.
    
    Args:
        X: Feature matrix
        contamination: Expected proportion of anomalies (0.0 to 0.5)
        n_estimators: Number of trees
        random_state: Random seed
        
    Returns:
        model: Trained Isolation Forest model
        scaler: Fitted StandardScaler
    """
    logger.info(f"Training Isolation Forest (contamination={contamination}, n_estimators={n_estimators})...")
    
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Train model
    model = IsolationForest(
        contamination=contamination,
        n_estimators=n_estimators,
        random_state=random_state,
        n_jobs=-1
    )
    model.fit(X_scaled)
    
    logger.info("Isolation Forest training complete")
    
    return model, scaler


def classify_anomaly_types(
    X: np.ndarray,
    anomaly_mask: np.ndarray,
    n_clusters: int = 5,
    random_state: int = 42
) -> np.ndarray:
    """
    Classify anomaly types using clustering on anomalous samples.
    
    Args:
        X: Feature matrix
        anomaly_mask: Boolean array indicating anomalies
        n_clusters: Number of anomaly types to identify
        
    Returns:
        anomaly_types: Array of anomaly type labels (0 = normal, 1-N = anomaly types)
    """
    logger.info(f"Classifying anomaly types (n_clusters={n_clusters})...")
    
    # Initialize with all normal (0)
    anomaly_types = np.zeros(len(X), dtype=int)
    
    # Get anomalous samples
    anomaly_indices = np.where(anomaly_mask)[0]
    n_anomalies = len(anomaly_indices)
    
    if n_anomalies == 0:
        logger.warning("No anomalies found, skipping classification")
        return anomaly_types
    
    if n_anomalies < n_clusters:
        logger.warning(f"Fewer anomalies ({n_anomalies}) than clusters ({n_clusters}), using {n_anomalies} clusters")
        n_clusters = n_anomalies
    
    if n_clusters <= 1:
        # If only one cluster, all anomalies are type 1
        anomaly_types[anomaly_indices] = 1
        return anomaly_types
    
    # Extract anomaly features
    X_anomalies = X[anomaly_indices]
    
    # Handle NaN values - fill with column mean
    from sklearn.impute import SimpleImputer
    if np.isnan(X_anomalies).any():
        logger.warning("Found NaN values in anomaly features, filling with column mean")
        imputer = SimpleImputer(strategy='mean')
        X_anomalies = imputer.fit_transform(X_anomalies)
    
    # Use PCA for dimensionality reduction before clustering
    n_components = min(50, X_anomalies.shape[1], n_anomalies - 1)
    if n_components > 0:
        pca = PCA(n_components=n_components, random_state=random_state)
        X_anomalies_reduced = pca.fit_transform(X_anomalies)
    else:
        X_anomalies_reduced = X_anomalies
    
    # Cluster anomalies
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state, n_init=10)
    cluster_labels = kmeans.fit_predict(X_anomalies_reduced)
    
    # Assign anomaly types (1 to n_clusters)
    anomaly_types[anomaly_indices] = cluster_labels + 1
    
    # Log distribution
    unique, counts = np.unique(anomaly_types[anomaly_indices], return_counts=True)
    logger.info(f"Anomaly type distribution:")
    for atype, count in zip(unique, counts):
        logger.info(f"  Type {atype}: {count} anomalies ({count/n_anomalies*100:.1f}%)")
    
    return anomaly_types


def predict_anomalies(
    model: IsolationForest,
    scaler: StandardScaler,
    X: np.ndarray
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Predict anomalies using trained model.
    
    Args:
        model: Trained Isolation Forest model
        scaler: Fitted StandardScaler
        X: Feature matrix
        
    Returns:
        predictions: Array of predictions (-1 = anomaly, 1 = normal)
        scores: Anomaly scores (lower = more anomalous)
    """
    logger.info("Predicting anomalies...")
    
    # Scale features
    X_scaled = scaler.transform(X)
    
    # Predict
    predictions = model.predict(X_scaled)
    scores = model.score_samples(X_scaled)
    
    # Convert to boolean (True = anomaly)
    anomaly_mask = predictions == -1
    
    n_anomalies = np.sum(anomaly_mask)
    logger.info(f"Detected {n_anomalies:,} anomalies ({n_anomalies/len(X)*100:.2f}%)")
    
    return predictions, scores


def save_model(
    model: IsolationForest,
    scaler: StandardScaler,
    feature_cols: list,
    output_dir: Path,
    metadata: Optional[Dict] = None,
    encoders: Optional[Dict] = None
):
    """Save trained model, scaler, and encoders."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save model
    model_path = output_dir / "isolation_forest.joblib"
    joblib.dump(model, model_path)
    logger.info(f"Saved model to {model_path}")
    
    # Save scaler
    scaler_path = output_dir / "scaler.joblib"
    joblib.dump(scaler, scaler_path)
    logger.info(f"Saved scaler to {scaler_path}")
    
    # Save encoders if provided
    if encoders:
        encoders_path = output_dir / "label_encoders.joblib"
        joblib.dump(encoders, encoders_path)
        logger.info(f"Saved label encoders to {encoders_path}")
    
    # Save feature columns
    feature_cols_path = output_dir / "feature_columns.json"
    with open(feature_cols_path, 'w') as f:
        json.dump(feature_cols, f, indent=2)
    logger.info(f"Saved feature columns to {feature_cols_path}")
    
    # Save metadata
    if metadata:
        metadata_path = output_dir / "model_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        logger.info(f"Saved model metadata to {metadata_path}")


def train_model(
    input_path: Path,
    output_dir: Path,
    contamination: float = 0.1,
    n_clusters: int = 5,
    n_estimators: int = 100,
    random_state: int = 42,
    train_split: float = 0.8
) -> Dict:
    """
    Main training function.
    
    Args:
        input_path: Path to vectorized parquet file
        output_dir: Directory to save model and results
        contamination: Expected proportion of anomalies
        n_clusters: Number of anomaly types
        n_estimators: Number of trees for Isolation Forest
        random_state: Random seed
        train_split: Proportion of sequences to use for training (default: 0.8)
        
    Returns:
        Dictionary with training results
    """
    # Load data
    df = load_vectorized_data(input_path)
    
    # Split by sequence_id to avoid data leakage
    unique_sequences = df["sequence_id"].unique().to_list()
    n_train = int(len(unique_sequences) * train_split)
    
    # Shuffle sequences for random split
    np.random.seed(random_state)
    np.random.shuffle(unique_sequences)
    
    train_sequences = unique_sequences[:n_train]
    test_sequences = unique_sequences[n_train:]
    
    logger.info(f"Splitting data by sequence:")
    logger.info(f"  Training sequences: {len(train_sequences)} ({len(train_sequences)/len(unique_sequences)*100:.1f}%)")
    logger.info(f"  Test sequences: {len(test_sequences)} ({len(test_sequences)/len(unique_sequences)*100:.1f}%)")
    
    # Split data
    train_df = df.filter(pl.col("sequence_id").is_in(train_sequences))
    test_df = df.filter(pl.col("sequence_id").is_in(test_sequences))
    
    logger.info(f"  Training cycles: {len(train_df):,}")
    logger.info(f"  Test cycles: {len(test_df):,}")
    
    # Prepare features - fit encoders on training set
    X_train, metadata_train, encoders = prepare_features(train_df)
    
    # Apply same encoders to test set for consistency
    X_test, metadata_test, _ = prepare_features(test_df, encoders=encoders)
    
    # Get feature columns (matching what was actually used in prepare_features)
    # This should match the filtered columns from prepare_features
    exclude_cols = ["sequence_id", "cycle_id", "cycle_datetime"]
    numeric_dtypes = [pl.Float64, pl.Float32, pl.Int64, pl.Int32]
    categorical_features = ["casting_program", "slabs_grade"]
    
    feature_cols = []
    for col in df.columns:
        if col not in exclude_cols:
            dtype = df[col].dtype
            if dtype in numeric_dtypes or col in categorical_features:
                feature_cols.append(col)
    
    # Train model on training set only
    logger.info("Training model on training set...")
    model, scaler = train_isolation_forest(
        X_train, 
        contamination=contamination,
        n_estimators=n_estimators,
        random_state=random_state
    )
    
    # Predict on both train and test sets
    logger.info("Predicting on training set...")
    train_predictions, train_scores = predict_anomalies(model, scaler, X_train)
    train_anomaly_mask = train_predictions == -1
    
    logger.info("Predicting on test set...")
    test_predictions, test_scores = predict_anomalies(model, scaler, X_test)
    test_anomaly_mask = test_predictions == -1
    
    # Classify anomaly types (use training anomalies to fit the classifier)
    logger.info("Classifying anomaly types...")
    train_anomaly_types = classify_anomaly_types(X_train, train_anomaly_mask, n_clusters=n_clusters, random_state=random_state)
    
    # For test set, we need to classify using the same approach
    # We'll use the training anomalies to fit a classifier, then apply to test
    test_anomaly_types = classify_anomaly_types(X_test, test_anomaly_mask, n_clusters=n_clusters, random_state=random_state)
    
    # Create results DataFrames
    train_results = metadata_train.with_columns([
        pl.Series("is_anomaly", train_anomaly_mask),
        pl.Series("anomaly_score", train_scores),
        pl.Series("anomaly_type", train_anomaly_types),
        pl.Series("split", ["train"] * len(metadata_train)),
    ])
    
    test_results = metadata_test.with_columns([
        pl.Series("is_anomaly", test_anomaly_mask),
        pl.Series("anomaly_score", test_scores),
        pl.Series("anomaly_type", test_anomaly_types),
        pl.Series("split", ["test"] * len(metadata_test)),
    ])
    
    # Combine results
    results = pl.concat([train_results, test_results])
    
    # Add feature attribution for anomalies (without port information - add later via post-processing)
    logger.info("Calculating feature attribution for anomalies...")
    X_all = np.vstack([X_train, X_test])
    
    # Calculate feature attribution (simplified - no port info during training)
    from model_training.feature_attribution import calculate_feature_attribution
    anomaly_mask = results["is_anomaly"].to_numpy()
    feature_deviations, top_features = calculate_feature_attribution(
        X_all, anomaly_mask, feature_cols, scaler, top_n=5
    )
    
    # Add top features and deviations to results
    for i in range(5):
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
    
    # Save results
    output_dir.mkdir(parents=True, exist_ok=True)
    results_path = output_dir / "anomaly_predictions.parquet"
    results.write_parquet(results_path)
    logger.info(f"Saved predictions to {results_path}")
    
    # Save test set anomalies only as CSV (with all columns)
    test_results = results.filter(pl.col("split") == "test")
    test_anomalies = test_results.filter(pl.col("is_anomaly")).sort(["sequence_id", "cycle_id"])
    if len(test_anomalies) > 0:
        test_anomalies_csv_path = output_dir / "test_set_anomalies.csv"
        test_anomalies.write_csv(test_anomalies_csv_path)
        logger.info(f"Saved test set anomalies ({len(test_anomalies):,} rows) to {test_anomalies_csv_path}")
    
    # Characterize anomaly types by analyzing feature differences
    anomaly_type_characteristics = {}
    if np.sum(test_anomaly_mask) > 0:
        # Get normal samples for comparison
        normal_mask = ~test_anomaly_mask
        X_normal = X_test[normal_mask]
        
        # Calculate mean features for normal samples
        normal_mean = np.nanmean(X_normal, axis=0)
        
        # For each anomaly type, calculate mean features and differences from normal
        unique_types = np.unique(test_anomaly_types[test_anomaly_mask])
        for atype in unique_types:
            type_mask = (test_anomaly_types == atype) & test_anomaly_mask
            X_type = X_test[type_mask]
            
            # Calculate mean features for this type
            type_mean = np.nanmean(X_type, axis=0)
            
            # Calculate difference from normal (normalized by normal std)
            normal_std = np.nanstd(X_normal, axis=0)
            type_diff = (type_mean - normal_mean) / (normal_std + 1e-6)
            
            # Find top features that differ most (absolute difference)
            top_indices = np.argsort(np.abs(type_diff))[-10:][::-1]  # Top 10
            
            # Get feature names
            top_features = [feature_cols[i] for i in top_indices]
            top_differences = [float(type_diff[i]) for i in top_indices]
            
            anomaly_type_characteristics[f"type_{atype}"] = {
                "count": int(np.sum(type_mask)),
                "top_differentiating_features": {
                    feature: {
                        "difference_from_normal": diff,
                        "interpretation": "higher_than_normal" if diff > 0 else "lower_than_normal"
                    }
                    for feature, diff in zip(top_features, top_differences)
                }
            }
    
    # Create evaluation summary report
    eval_summary = {
        "evaluation_summary": {
            "test_sequences": len(test_sequences),
            "test_cycles": len(test_df),
            "test_anomalies": int(np.sum(test_anomaly_mask)),
            "test_anomaly_rate": f"{np.sum(test_anomaly_mask)/len(X_test)*100:.2f}%",
            "anomaly_types_detected": int(len(np.unique(test_anomaly_types[test_anomaly_mask]))) if np.sum(test_anomaly_mask) > 0 else 0,
        },
        "anomaly_distribution_by_type": {},
        "anomaly_type_characteristics": anomaly_type_characteristics
    }
    
    if np.sum(test_anomaly_mask) > 0:
        unique_types, counts = np.unique(test_anomaly_types[test_anomaly_mask], return_counts=True)
        for atype, count in zip(unique_types, counts):
            eval_summary["anomaly_distribution_by_type"][f"type_{atype}"] = {
                "count": int(count),
                "percentage": f"{count/np.sum(test_anomaly_mask)*100:.1f}%"
            }
    
    # Add cast_length statistics for anomalies
    if len(test_anomalies) > 0:
        cast_length_stats = test_anomalies.select([
            pl.col("cast_length_m").min().alias("min_cast_length"),
            pl.col("cast_length_m").max().alias("max_cast_length"),
            pl.col("cast_length_m").mean().alias("mean_cast_length"),
            pl.col("cast_length_m").median().alias("median_cast_length"),
        ])
        eval_summary["cast_length_statistics"] = cast_length_stats.to_dicts()[0]
        
        # Add cast_length statistics per anomaly type
        cast_length_by_type = {}
        for atype in unique_types:
            type_anomalies = test_anomalies.filter(pl.col("anomaly_type") == atype)
            if len(type_anomalies) > 0:
                type_stats = type_anomalies.select([
                    pl.col("cast_length_m").min().alias("min_cast_length"),
                    pl.col("cast_length_m").max().alias("max_cast_length"),
                    pl.col("cast_length_m").mean().alias("mean_cast_length"),
                    pl.col("cast_length_m").median().alias("median_cast_length"),
                ])
                cast_length_by_type[f"type_{atype}"] = type_stats.to_dicts()[0]
        eval_summary["cast_length_by_type"] = cast_length_by_type
    
    eval_summary_path = output_dir / "test_set_evaluation_summary.json"
    with open(eval_summary_path, 'w') as f:
        json.dump(eval_summary, f, indent=2)
    logger.info(f"Saved evaluation summary to {eval_summary_path}")
    
    # Save model
    model_metadata = {
        "model_type": "IsolationForest",
        "contamination": contamination,
        "n_estimators": n_estimators,
        "n_features": X_train.shape[1],
        "n_train_samples": X_train.shape[0],
        "n_test_samples": X_test.shape[0],
        "n_train_sequences": len(train_sequences),
        "n_test_sequences": len(test_sequences),
        "train_split": train_split,
        "n_train_anomalies": int(np.sum(train_anomaly_mask)),
        "n_test_anomalies": int(np.sum(test_anomaly_mask)),
        "train_anomaly_rate": float(np.sum(train_anomaly_mask) / len(X_train)),
        "test_anomaly_rate": float(np.sum(test_anomaly_mask) / len(X_test)),
        "n_anomaly_types": n_clusters,
        "trained_at": datetime.now().isoformat(),
    }
    save_model(model, scaler, feature_cols, output_dir, model_metadata, encoders)
    
    # Print summary
    print("\n=== Training Summary ===")
    print(f"Training cycles: {len(train_df):,} ({len(train_sequences)} sequences)")
    print(f"Test cycles: {len(test_df):,} ({len(test_sequences)} sequences)")
    print(f"Train anomalies: {np.sum(train_anomaly_mask):,} ({np.sum(train_anomaly_mask)/len(X_train)*100:.2f}%)")
    print(f"Test anomalies: {np.sum(test_anomaly_mask):,} ({np.sum(test_anomaly_mask)/len(X_test)*100:.2f}%)")
    print(f"Anomaly types: {len(np.unique(train_anomaly_types[train_anomaly_mask]))}")
    print(f"Model saved to: {output_dir}")
    
    return {
        "n_train_samples": len(X_train),
        "n_test_samples": len(X_test),
        "n_train_anomalies": int(np.sum(train_anomaly_mask)),
        "n_test_anomalies": int(np.sum(test_anomaly_mask)),
        "train_anomaly_rate": float(np.sum(train_anomaly_mask) / len(X_train)),
        "test_anomaly_rate": float(np.sum(test_anomaly_mask) / len(X_test)),
        "anomaly_types": int(len(np.unique(train_anomaly_types[train_anomaly_mask]))),
    }


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description='Train anomaly detection model on vectorized casting data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Train with default parameters
  python model_training/train.py --input feature_engineering/vectorized/vectorized.parquet --output model_training/models/
  
  # Train with custom contamination rate
  python model_training/train.py --input feature_engineering/vectorized/vectorized.parquet --output model_training/models/ --contamination 0.05
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input vectorized parquet file'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='model_training/models/',
        help='Output directory for model and results (default: model_training/models/)'
    )
    
    parser.add_argument(
        '--contamination', '-c',
        type=float,
        default=0.1,
        help='Expected proportion of anomalies (0.0 to 0.5, default: 0.1)'
    )
    
    parser.add_argument(
        '--n-clusters', '-n',
        type=int,
        default=5,
        help='Number of anomaly types to identify (default: 5)'
    )
    
    parser.add_argument(
        '--n-estimators',
        type=int,
        default=100,
        help='Number of trees for Isolation Forest (default: 100)'
    )
    
    parser.add_argument(
        '--random-state',
        type=int,
        default=42,
        help='Random seed (default: 42)'
    )
    
    parser.add_argument(
        '--train-split',
        type=float,
        default=0.8,
        help='Proportion of sequences for training (default: 0.8)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        input_path = Path(args.input)
        output_dir = Path(args.output)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input path does not exist: {input_path}")
        
        if not (0.0 < args.contamination <= 0.5):
            raise ValueError(f"Contamination must be between 0.0 and 0.5, got {args.contamination}")
        
        if not (0.0 < args.train_split < 1.0):
            raise ValueError(f"Train split must be between 0.0 and 1.0, got {args.train_split}")
        
        # Train model
        results = train_model(
            input_path=input_path,
            output_dir=output_dir,
            contamination=args.contamination,
            n_clusters=args.n_clusters,
            n_estimators=args.n_estimators,
            random_state=args.random_state,
            train_split=args.train_split
        )
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())


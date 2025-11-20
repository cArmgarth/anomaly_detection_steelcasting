#!/usr/bin/env python3
"""
Feature Attribution

Calculates which features contribute most to each anomaly detection.
"""

import numpy as np
from typing import Tuple, List
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer


def calculate_feature_attribution(
    X: np.ndarray,
    anomaly_mask: np.ndarray,
    feature_cols: List[str],
    scaler: StandardScaler,
    normal_samples: np.ndarray = None,
    top_n: int = 5
) -> Tuple[np.ndarray, List[List[str]]]:
    """
    Calculate feature attribution for each anomaly.
    
    For each anomaly, identifies which features deviate most from normal.
    
    Args:
        X: Feature matrix (all samples)
        anomaly_mask: Boolean array indicating anomalies
        feature_cols: List of feature names
        scaler: Fitted StandardScaler
        normal_samples: Optional normal samples for comparison (if None, uses non-anomalies)
        top_n: Number of top contributing features to return per anomaly
        
    Returns:
        feature_deviations: Array of shape (n_samples, n_features) with deviation scores
        top_features: List of lists, each containing top N feature names for each sample
    """
    # Get normal samples (for comparison baseline)
    if normal_samples is None:
        normal_mask = ~anomaly_mask
        if np.sum(normal_mask) == 0:
            normal_samples = X
        else:
            normal_samples = X[normal_mask]
    
    # Handle NaN values
    # Convert to float if needed
    if X.dtype != np.float64:
        X = X.astype(np.float64)
    if normal_samples.dtype != np.float64:
        normal_samples = normal_samples.astype(np.float64)
    
    if np.isnan(X).any() or np.isnan(normal_samples).any():
        imputer = SimpleImputer(strategy='mean')
        X = imputer.fit_transform(X)
        normal_samples = imputer.transform(normal_samples)
    
    # Calculate normal distribution statistics
    normal_mean = np.nanmean(normal_samples, axis=0)
    normal_std = np.nanstd(normal_samples, axis=0)
    
    # Avoid division by zero
    normal_std = np.where(normal_std < 1e-10, 1.0, normal_std)
    
    # Calculate z-scores (how many standard deviations away from normal)
    feature_deviations = np.abs((X - normal_mean) / normal_std)
    
    # For each sample, get top N features by deviation
    top_features = []
    for i in range(len(X)):
        top_indices = np.argsort(feature_deviations[i])[-top_n:][::-1]
        top_feature_names = [feature_cols[idx] for idx in top_indices]
        top_features.append(top_feature_names)
    
    return feature_deviations, top_features


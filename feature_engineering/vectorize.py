#!/usr/bin/env python3
"""
Vectorization for Anomaly Detection

Converts time series casting data into per-cycle feature vectors.
One cycle = one complete measurement cycle of ports 17-32.

Each cycle produces:
- Aggregated temperature features from ports 17-32
- Process variable features at cycle time
- Temporal/contextual features
- Cast length for anomaly localization
"""

import polars as pl
import numpy as np
import argparse
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ports 17-32 (inclusive) should be aggregated as one reading
PORT_AGGREGATE_START = 17
PORT_AGGREGATE_END = 32
PORT_AGGREGATE_RANGE = list(range(PORT_AGGREGATE_START, PORT_AGGREGATE_END + 1))


def identify_cycles(df: pl.DataFrame) -> pl.DataFrame:
    """
    Identify cycles: groups of consecutive ports 17-32 within a sequence.
    
    A cycle starts when port == 17 and includes all ports up to and including port 32.
    
    Args:
        df: DataFrame with port column
        
    Returns:
        DataFrame with cycle_id column added
    """
    # Filter to ports 17-32
    port_df = df.filter(
        (pl.col("port") >= PORT_AGGREGATE_START) & 
        (pl.col("port") <= PORT_AGGREGATE_END)
    ).sort(["sequence_id", "datetime", "port"])
    
    # Identify cycles: a new cycle starts every time we see port 17
    # A cycle is a complete pass through ports 17-32
    # Since we're filtering to only ports 17-32, every time we see port 17, it's a new cycle
    port_df = port_df.with_columns([
        # Mark cycle starts (port == 17)
        (pl.col("port") == PORT_AGGREGATE_START).alias("is_cycle_start"),
        # Port number from previous row (to detect when we wrap around or restart)
        pl.col("port").shift(1).over("sequence_id").alias("prev_port"),
    ])
    
    # Create cycle_id: new cycle when port == 17
    # Also handle edge cases: first row in sequence, or when previous port was outside 17-32
    port_df = port_df.with_columns([
        (
            (pl.col("is_cycle_start")) &
            (
                pl.col("prev_port").is_null() |  # First row in sequence
                (pl.col("prev_port") < PORT_AGGREGATE_START) |  # Previous was before port 17
                (pl.col("prev_port") > PORT_AGGREGATE_END) |  # Previous was after port 32 (wrapped around)
                (pl.col("prev_port") == PORT_AGGREGATE_END)  # Previous was port 32, now we're at 17 (new cycle)
            )
        ).alias("new_cycle"),
    ])
    
    # Create cycle_id by cumulative sum of new_cycle markers per sequence
    port_df = port_df.with_columns([
        pl.col("new_cycle").cum_sum().over("sequence_id").alias("cycle_id")
    ])
    
    return port_df.drop(["is_cycle_start", "prev_port", "new_cycle"])


def extract_temperature_features(port_df: pl.DataFrame) -> pl.DataFrame:
    """
    Extract temperature features from ports 17-32 per cycle.
    
    Args:
        port_df: DataFrame with cycle_id already identified
        
    Returns:
        DataFrame with aggregated temperature features per cycle
    """
    logger.info("Extracting temperature features from ports 17-32...")
    
    # Get all temperature columns
    temp_cols = [f"T_{i}" for i in range(71)]
    available_temp_cols = [col for col in temp_cols if col in port_df.columns]
    
    if not available_temp_cols:
        raise ValueError("No temperature columns (T_0 to T_70) found in data")
    
    logger.info(f"Found {len(available_temp_cols)} temperature columns")
    
    # Aggregate temperature features per cycle
    # For each temperature sensor, compute statistics across all ports 17-32 in the cycle
    agg_exprs = []
    
    for temp_col in available_temp_cols:
        agg_exprs.extend([
            pl.col(temp_col).mean().alias(f"{temp_col}_mean"),
            pl.col(temp_col).std().alias(f"{temp_col}_std"),
            pl.col(temp_col).min().alias(f"{temp_col}_min"),
            pl.col(temp_col).max().alias(f"{temp_col}_max"),
        ])
    
    # Also compute overall statistics across all sensors and ports in the cycle
    all_temp_values = []
    for temp_col in available_temp_cols:
        all_temp_values.append(pl.col(temp_col))
    
    # Aggregate per cycle
    cycle_features = (
        port_df
        .group_by(["sequence_id", "cycle_id"])
        .agg(agg_exprs)
    )
    
    # Compute overall statistics from sensor-level aggregations
    # Collect all sensor means, mins, maxes to compute overall stats
    sensor_mean_cols = [f"{col}_mean" for col in available_temp_cols]
    sensor_min_cols = [f"{col}_min" for col in available_temp_cols]
    sensor_max_cols = [f"{col}_max" for col in available_temp_cols]
    
    cycle_features = cycle_features.with_columns([
        # Overall mean: mean of all sensor means
        pl.concat_list([pl.col(col) for col in sensor_mean_cols]).alias("sensor_means"),
        # Overall min: min of all sensor mins
        pl.concat_list([pl.col(col) for col in sensor_min_cols]).alias("sensor_mins"),
        # Overall max: max of all sensor maxes
        pl.concat_list([pl.col(col) for col in sensor_max_cols]).alias("sensor_maxes"),
    ]).with_columns([
        pl.col("sensor_means").list.mean().alias("temp_overall_mean"),
        pl.col("sensor_means").list.std().alias("temp_overall_std"),
        pl.col("sensor_mins").list.min().alias("temp_overall_min"),
        pl.col("sensor_maxes").list.max().alias("temp_overall_max"),
    ]).with_columns([
        # Spatial gradient features (detect sudden temperature changes)
        (pl.col("temp_overall_max") - pl.col("temp_overall_min")).alias("temp_spatial_range"),
        
        # Hot/cold spot indicators (normalized extremes)
        # Hot spot severity: how many stds above mean is the max temp
        ((pl.col("temp_overall_max") - pl.col("temp_overall_mean")) / 
         (pl.col("temp_overall_std") + 1e-6)).alias("hot_spot_severity"),
        
        # Cold spot severity: how many stds below mean is the min temp
        ((pl.col("temp_overall_mean") - pl.col("temp_overall_min")) / 
         (pl.col("temp_overall_std") + 1e-6)).alias("cold_spot_severity"),
    ]).drop(["sensor_means", "sensor_mins", "sensor_maxes"])
    
    # Add per-sensor spatial range features (temperature range per sensor across ports)
    # This captures spatial variability for each sensor
    sensor_range_exprs = []
    for temp_col in available_temp_cols:
        # Range = max - min across ports 17-32 for this sensor
        sensor_range_exprs.append(
            (pl.col(f"{temp_col}_max") - pl.col(f"{temp_col}_min")).alias(f"{temp_col}_range")
        )
    
    cycle_features = cycle_features.with_columns(sensor_range_exprs)
    
    # Add average spatial gradient (average range across all sensors)
    # Compute mean and std of all sensor ranges
    range_exprs = [pl.col(f"{col}_range") for col in available_temp_cols]
    cycle_features = cycle_features.with_columns([
        pl.concat_list(range_exprs).alias("sensor_ranges"),
    ]).with_columns([
        pl.col("sensor_ranges").list.mean().alias("avg_spatial_gradient"),
        pl.col("sensor_ranges").list.std().alias("spatial_gradient_std"),
    ]).drop("sensor_ranges")
    
    return cycle_features


def extract_process_features(port_df: pl.DataFrame) -> pl.DataFrame:
    """
    Extract process variable features at cycle timepoints.
    
    Args:
        port_df: DataFrame with cycle_id already identified
        
    Returns:
        DataFrame with process features per cycle
    """
    logger.info("Extracting process variable features...")
    
    # Process variables to extract (only those specified for the model)
    process_vars = [
        "rec_casting_speed",  # casting speed
        "deltatloose",
        "ar_to_sen_flow",
        "ar_to_sen_pressure",
        "tundish_pos_rel",
        "mould_level",
    ]
    
    available_process_vars = [col for col in process_vars if col in port_df.columns]
    
    # Get schema to check dtypes
    schema = port_df.schema
    
    # For each cycle, take mean/std of process variables across the cycle
    agg_exprs = []
    for col in available_process_vars:
        # For numeric columns, compute mean and std
        dtype = schema[col]
        if dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
            agg_exprs.extend([
                pl.col(col).mean().alias(f"{col}_mean"),
                pl.col(col).std().alias(f"{col}_std"),
            ])
        else:
            # For string/categorical, just take first value
            agg_exprs.append(pl.col(col).first().alias(f"{col}"))
    
    # Get cast_length at cycle start (for anomaly localization)
    # Note: cast_length_m is also included in temporal features, but we need it here
    # for the join. We'll handle duplicates later.
    cycle_process = (
        port_df
        .group_by(["sequence_id", "cycle_id"])
        .agg([
            *agg_exprs,
            # Get cast_length at cycle start (port 17) - for anomaly localization
            pl.col("cast_length_m").first().alias("cast_length_m"),
        ])
    )
    
    return cycle_process


def extract_temporal_features(port_df: pl.DataFrame) -> pl.DataFrame:
    """
    Extract temporal/contextual features per cycle.
    
    Args:
        port_df: DataFrame with cycle_id already identified (ports 17-32 only)
        
    Returns:
        DataFrame with temporal features per cycle
    """
    logger.info("Extracting temporal/contextual features...")
    
    # Build aggregation list - only include features specified for the model
    # Keep sequence_id and cast_length_m for anomaly localization/tracking
    # Note: sequence_id is already in group_by, so we don't need to select it again
    agg_list = [
        # Tracking metadata (for anomaly localization)
        pl.col("cast_length_m").first().alias("cast_length_m"),
        pl.col("datetime").first().alias("cycle_datetime"),
        
        # Model features
        pl.col("casting_program").first().alias("casting_program"),
        pl.col("slabs_grade").first().alias("slabs_grade"),
    ]
    
    # Compute temporal features per cycle
    cycle_temporal = (
        port_df
        .group_by(["sequence_id", "cycle_id"])
        .agg(agg_list)
    )
    
    return cycle_temporal


def add_rolling_window_features(
    vectorized: pl.DataFrame,
    window_sizes: list = [3, 5],
    include_deltas: bool = True,
    include_trends: bool = True
) -> pl.DataFrame:
    """
    Add rolling window features to capture temporal patterns.
    
    Args:
        vectorized: DataFrame with per-cycle features (must be sorted by sequence_id, cycle_id)
        window_sizes: List of window sizes for rolling statistics (e.g., [3, 5] for last 3 and 5 cycles)
        include_deltas: Whether to include delta features (current - previous cycle)
        include_trends: Whether to include trend indicators (slope/rate of change)
        
    Returns:
        DataFrame with additional rolling window features
    """
    logger.info("Adding rolling window features...")
    
    # Ensure data is sorted by sequence_id and cycle_id
    vectorized = vectorized.sort(["sequence_id", "cycle_id"])
    
    # Identify numeric feature columns (exclude metadata columns)
    exclude_cols = {
        "sequence_id", "cycle_id", "cycle_datetime", 
        "casting_program", "slabs_grade", "cast_length_m"
    }
    
    all_numeric_cols = [
        col for col in vectorized.columns 
        if col not in exclude_cols and 
        vectorized[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]
    ]
    
    # Select key features for rolling windows to avoid memory issues
    # Focus on: temperature sensor means (most important), key process variables, and overall temp features
    key_features = []
    
    # Temperature sensor MEAN features only (most important, reduces feature count)
    key_features.extend([col for col in all_numeric_cols if col.startswith("T_") and col.endswith("_mean")])
    
    # Overall temperature features (mean, std, min, max)
    key_features.extend([col for col in all_numeric_cols if col.startswith("temp_overall")])
    
    # Spatial features (important for anomaly detection)
    key_features.extend([col for col in all_numeric_cols if "spatial" in col or "gradient" in col])
    
    # Key process variables (mean only to reduce features)
    process_vars = [
        "rec_casting_speed_mean", "deltatloose_mean", "ar_to_sen_flow_mean", 
        "ar_to_sen_pressure_mean", "tundish_pos_rel_mean", "mould_level_mean"
    ]
    key_features.extend([col for col in all_numeric_cols if col in process_vars])
    
    # Remove duplicates and ensure we only use features that exist
    numeric_cols = list(set([col for col in key_features if col in all_numeric_cols]))
    
    logger.info(f"Adding rolling window features for {len(numeric_cols)} key numeric columns (out of {len(all_numeric_cols)} total)")
    
    new_columns = {}
    
    # 1. Rolling window statistics (mean, std) for each window size
    for window_size in window_sizes:
        logger.info(f"  Adding rolling statistics for window size {window_size}...")
        for col in numeric_cols:
            # Rolling mean over last N cycles (within same sequence)
            new_columns[f"{col}_rolling_mean_{window_size}"] = (
                pl.col(col)
                .rolling_mean(window_size=window_size, min_samples=1)
                .over("sequence_id")
            )
            
            # Rolling std over last N cycles
            new_columns[f"{col}_rolling_std_{window_size}"] = (
                pl.col(col)
                .rolling_std(window_size=window_size, min_samples=1)
                .over("sequence_id")
            )
    
    # 2. Delta features (change from previous cycle)
    if include_deltas:
        logger.info("  Adding delta features (current - previous cycle)...")
        for col in numeric_cols:
            # Delta: current - previous
            new_columns[f"{col}_delta"] = (
                pl.col(col) - pl.col(col).shift(1).over("sequence_id")
            )
            
            # Relative delta: (current - previous) / (previous + epsilon)
            new_columns[f"{col}_delta_pct"] = (
                (pl.col(col) - pl.col(col).shift(1).over("sequence_id")) /
                (pl.col(col).shift(1).over("sequence_id").abs() + 1e-6)
            )
    
    # 3. Trend indicators
    if include_trends:
        logger.info("  Adding trend indicators...")
        for col in numeric_cols:
            # Slope over last 3 cycles (simple linear trend)
            # Using a 3-cycle window for trend calculation
            trend_window = 3
            
            # Calculate trend as (current - value_3_cycles_ago) / 3
            new_columns[f"{col}_trend_3cycle"] = (
                (pl.col(col) - pl.col(col).shift(trend_window).over("sequence_id")) / trend_window
            )
            
            # Acceleration: change in delta (second derivative)
            # acceleration = delta(current) - delta(previous)
            if include_deltas:
                delta_col = f"{col}_delta"
                new_columns[f"{col}_acceleration"] = (
                    pl.col(delta_col) - pl.col(delta_col).shift(1).over("sequence_id")
                )
    
    # 4. Rolling min/max (for detecting extremes)
    for window_size in window_sizes:
        logger.info(f"  Adding rolling min/max for window size {window_size}...")
        for col in numeric_cols:
            new_columns[f"{col}_rolling_min_{window_size}"] = (
                pl.col(col)
                .rolling_min(window_size=window_size, min_samples=1)
                .over("sequence_id")
            )
            
            new_columns[f"{col}_rolling_max_{window_size}"] = (
                pl.col(col)
                .rolling_max(window_size=window_size, min_samples=1)
                .over("sequence_id")
            )
            
            # Deviation from rolling mean (how far is current from recent average)
            new_columns[f"{col}_dev_from_rolling_mean_{window_size}"] = (
                pl.col(col) - new_columns[f"{col}_rolling_mean_{window_size}"]
            )
    
    # Add all new columns in stages to handle dependencies
    if new_columns:
        # First, add rolling statistics and deltas
        stage1_cols = {}
        stage2_cols = {}
        
        for name, expr in new_columns.items():
            if "dev_from_rolling_mean" in name or "acceleration" in name:
                # These depend on other columns, add in stage 2
                stage2_cols[name] = expr
            else:
                stage1_cols[name] = expr
        
        # Stage 1: Add rolling stats, deltas, trends (that don't depend on other new columns)
        if stage1_cols:
            vectorized = vectorized.with_columns([
                expr.alias(name) if isinstance(expr, pl.Expr) else pl.lit(expr).alias(name)
                for name, expr in stage1_cols.items()
            ])
        
        # Stage 2: Add columns that depend on stage 1 columns
        if stage2_cols:
            for name, expr in stage2_cols.items():
                if "dev_from_rolling_mean" in name:
                    # Extract the base column name and window size
                    base_col = name.replace("_dev_from_rolling_mean_3", "").replace("_dev_from_rolling_mean_5", "")
                    window_size = "3" if "_3" in name else "5"
                    rolling_mean_col = f"{base_col}_rolling_mean_{window_size}"
                    
                    # Create expression: current - rolling_mean
                    vectorized = vectorized.with_columns([
                        (pl.col(base_col) - pl.col(rolling_mean_col)).alias(name)
                    ])
                elif "acceleration" in name:
                    # Extract base column name
                    base_col = name.replace("_acceleration", "")
                    delta_col = f"{base_col}_delta"
                    
                    # Create expression: delta(current) - delta(previous)
                    vectorized = vectorized.with_columns([
                        (pl.col(delta_col) - pl.col(delta_col).shift(1).over("sequence_id")).alias(name)
                    ])
        
        logger.info(f"Added {len(new_columns)} rolling window features")
    
    return vectorized


def vectorize_cycles(
    input_path: Path,
    output_path: Optional[Path] = None
) -> pl.DataFrame:
    """
    Convert time series casting data to per-cycle feature vectors.
    
    One cycle = one complete measurement cycle of ports 17-32.
    Each cycle produces one feature vector for anomaly detection.
    
    Args:
        input_path: Path to cleaned parquet file(s)
        output_path: Optional path to save vectorized data
        
    Returns:
        DataFrame with feature vectors (one row per cycle)
    """
    logger.info(f"Loading data from {input_path}")
    
    # Load data
    if input_path.is_file() and input_path.suffix == '.parquet':
        df = pl.scan_parquet(input_path).collect()
    elif input_path.is_dir():
        parquet_files = sorted(input_path.glob('*.parquet'))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {input_path}")
        logger.info(f"Found {len(parquet_files)} parquet files")
        df = pl.scan_parquet(parquet_files).collect()
    else:
        raise FileNotFoundError(f"Path {input_path} is neither a parquet file nor a directory")
    
    logger.info(f"Loaded {len(df):,} rows")
    
    # Identify cycles (groups of ports 17-32)
    logger.info("Identifying cycles (ports 17-32)...")
    port_df = identify_cycles(df)
    
    logger.info(f"Rows with ports {PORT_AGGREGATE_START}-{PORT_AGGREGATE_END}: {len(port_df):,}")
    logger.info(f"Unique cycles identified: {port_df.select(pl.col('cycle_id').n_unique()).item()}")
    
    # Extract features per cycle
    logger.info("Extracting features per cycle...")
    
    # Temperature features
    temp_features = extract_temperature_features(port_df)
    logger.info(f"Temperature features: {len(temp_features.columns)} columns, {len(temp_features)} cycles")
    
    # Process features
    process_features = extract_process_features(port_df)
    logger.info(f"Process features: {len(process_features.columns)} columns, {len(process_features)} cycles")
    
    # Temporal features
    temporal_features = extract_temporal_features(port_df)
    logger.info(f"Temporal features: {len(temporal_features.columns)} columns, {len(temporal_features)} cycles")
    
    # Join all features
    logger.info("Joining all features...")
    vectorized = (
        temp_features
        .join(process_features, on=["sequence_id", "cycle_id"], how="inner")
        .join(temporal_features, on=["sequence_id", "cycle_id"], how="inner")
    )
    
    # Remove duplicate columns from joins
    # Keep: cast_length_m, sequence_id, cycle_datetime from temporal_features
    # Remove duplicates from process_features
    if "cast_length_m" in vectorized.columns:
        # Check if we have duplicates
        col_counts = {}
        for col in vectorized.columns:
            col_counts[col] = col_counts.get(col, 0) + 1
        
        # Remove duplicate cast_length_m (keep the one from temporal_features)
        if col_counts.get("cast_length_m", 0) > 1:
            # Drop the one from process_features (it will be named cast_length_m or cast_length_m_right)
            vectorized = vectorized.drop("cast_length_m_right") if "cast_length_m_right" in vectorized.columns else vectorized
    
    # Add rolling window features (using smaller window set to reduce memory)
    vectorized = add_rolling_window_features(
        vectorized,
        window_sizes=[3],  # Use only window size 3 to reduce features
        include_deltas=True,
        include_trends=True
    )
    
    # Ensure we have the required features in the final output
    # Features for the model: temp data, cast_length, casting speed, deltatloose, 
    # ar_to_sen_flow, ar_to_sen_pressure, tundish_pos_rel, mould_level, 
    # slabs_grade, casting_program
    # Plus tracking: sequence_id, cycle_datetime, cycle_id
    # Plus rolling window features (rolling means, deltas, trends, etc.)
    
    logger.info(f"Vectorized data: {len(vectorized):,} cycles, {len(vectorized.columns)} features")
    
    # Save if output path provided
    if output_path:
        output_path = Path(output_path)
        if output_path.suffix == '.parquet':
            # Save as single file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            vectorized.write_parquet(output_path)
            logger.info(f"Saved vectorized data to {output_path}")
        else:
            # Save as directory with parquet file
            output_path.mkdir(parents=True, exist_ok=True)
            single_output = output_path / "vectorized.parquet"
            vectorized.write_parquet(single_output)
            logger.info(f"Saved vectorized data to {single_output}")
    
    return vectorized


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description='Vectorize casting data into per-cycle feature vectors for anomaly detection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Vectorize all cleaned data
  python feature_engineering/vectorize.py --input data_ingest/data_cleaning/cleaned_data/ --output feature_engineering/vectorized/
  
  # Vectorize single file
  python feature_engineering/vectorize.py --input data_ingest/data_cleaning/cleaned_data/2025-09-02.parquet --output feature_engineering/vectorized/2025-09-02.parquet
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input cleaned parquet file or directory'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='feature_engineering/vectorized/',
        help='Output parquet file or directory (default: feature_engineering/vectorized/)'
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
        output_path = Path(args.output)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input path does not exist: {input_path}")
        
        # Vectorize data
        result_df = vectorize_cycles(input_path, output_path)
        
        print(f"\n=== Vectorization Summary ===")
        print(f"Input: {input_path}")
        print(f"Output: {output_path}")
        print(f"Cycles: {len(result_df):,}")
        print(f"Features: {len(result_df.columns)}")
        print(f"Sequences: {result_df['sequence_id'].n_unique()}")
        if 'cast_length_m' in result_df.columns:
            print(f"Cast length range: {result_df['cast_length_m'].min():.2f}m to {result_df['cast_length_m'].max():.2f}m")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())


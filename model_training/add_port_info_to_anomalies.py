#!/usr/bin/env python3
"""
Add Port Information to Anomaly Results

Post-processing script to add port location information to anomaly detection results.
This allows pinpointing anomalies on the sensor map.

Usage:
    python model_training/add_port_info_to_anomalies.py \
        --results model_training/models/test_set_anomalies.csv \
        --cleaned-data data_ingest/data_cleaning/cleaned_data/ \
        --output model_training/models/test_set_anomalies_with_ports.csv
"""

import polars as pl
import argparse
import json
from pathlib import Path
from typing import List, Dict
import re


def find_anomalous_ports_for_sensor(
    sequence_id: float,
    cycle_id: int,
    sensor_num: int,
    cleaned_data_dir: Path,
    max_ports: int = 3
) -> List[int]:
    """
    Find which port(s) had the most anomalous readings for a sensor in a specific cycle.
    
    Args:
        sequence_id: Sequence ID
        cycle_id: Cycle ID
        sensor_num: Sensor number (0-70, where T_0 = sensor 0)
        cleaned_data_dir: Directory with cleaned parquet files
        max_ports: Maximum number of ports to return
        
    Returns:
        List of port numbers with highest deviations (up to max_ports)
    """
    sensor_col = f"T_{sensor_num}"
    
    # Load cycle data
    parquet_files = sorted(cleaned_data_dir.glob("*.parquet"))
    
    for file in parquet_files:
        df = pl.read_parquet(file)
        if sequence_id not in df["sequence_id"].unique().to_list():
            continue
        
        seq_data = df.filter(pl.col("sequence_id") == sequence_id)
        
        # Filter to ports 17-32 and identify cycles
        port_df = seq_data.filter(
            (pl.col("port") >= 17) & (pl.col("port") <= 32)
        ).sort(["datetime", "port"])
        
        # Identify cycles (matching logic from vectorize.py)
        port_df = port_df.with_columns([
            (pl.col("port") == 17).alias("is_cycle_start"),
            pl.col("port").shift(1).over("sequence_id").alias("prev_port"),
        ]).with_columns([
            (
                (pl.col("is_cycle_start")) &
                (
                    pl.col("prev_port").is_null() |
                    (pl.col("prev_port") < 17) |
                    (pl.col("prev_port") > 32) |
                    (pl.col("prev_port") == 32)
                )
            ).alias("new_cycle"),
        ]).with_columns([
            pl.col("new_cycle").cum_sum().over("sequence_id").alias("cycle_id")
        ])
        
        cycle_data = port_df.filter(pl.col("cycle_id") == cycle_id)
        
        if len(cycle_data) == 0 or sensor_col not in cycle_data.columns:
            continue
        
        # Calculate cycle mean and std for this sensor
        cycle_mean = cycle_data[sensor_col].mean()
        cycle_std = cycle_data[sensor_col].std()
        
        if cycle_std is None or cycle_std < 1e-10:
            continue
        
        # Calculate deviations per port
        port_deviations = []
        for port in sorted(cycle_data["port"].unique().to_list()):
            port_data = cycle_data.filter(pl.col("port") == port)
            sensor_values = port_data[sensor_col].drop_nulls().to_list()
            
            if len(sensor_values) > 0:
                port_mean = sum(sensor_values) / len(sensor_values)
                deviation = abs((port_mean - cycle_mean) / (cycle_std + 1e-10))
                port_deviations.append((port, deviation))
        
        if not port_deviations:
            continue
        
        # Sort by deviation and get top ports (within 80% of max)
        port_deviations.sort(key=lambda x: x[1], reverse=True)
        if port_deviations:
            max_deviation = port_deviations[0][1]
            threshold = max_deviation * 0.8
            
            anomalous_ports = [
                port for port, dev in port_deviations
                if dev >= threshold
            ]
            
            return anomalous_ports[:max_ports]
    
    return []


def extract_sensor_from_feature_name(feat_name: str) -> int:
    """Extract sensor number from feature name (e.g., 'T_37_max' -> 37)."""
    match = re.match(r'T_(\d+)_(mean|std|min|max|range)', feat_name)
    if match:
        return int(match.group(1))
    return None


def add_port_info_to_anomalies(
    results_path: Path,
    cleaned_data_dir: Path,
    output_path: Path
):
    """
    Add port information to anomaly results.
    
    Args:
        results_path: Path to anomaly results CSV
        cleaned_data_dir: Directory with cleaned parquet files
        output_path: Path to save enhanced results
    """
    print(f"Loading anomaly results from {results_path}...")
    results = pl.read_csv(results_path)
    
    print(f"Found {len(results):,} anomalies")
    print("Adding port information...")
    
    # Add port information for each anomaly
    port_info_list = []
    
    for i, row in enumerate(results.iter_rows(named=True)):
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(results)} anomalies...")
        
        sequence_id = row["sequence_id"]
        cycle_id = row["cycle_id"]
        
        # Get top contributing features
        top_features = []
        for j in range(1, 6):  # top_feature_1 to top_feature_5
            feat_name = row.get(f"top_feature_{j}", "")
            if feat_name:
                top_features.append(feat_name)
        
        # Find ports for each contributing sensor
        sensor_ports: Dict[str, List[int]] = {}
        
        for feat_name in top_features:
            sensor_num = extract_sensor_from_feature_name(feat_name)
            if sensor_num is not None:
                sensor_key = f"T_{sensor_num}"
                if sensor_key not in sensor_ports:
                    ports = find_anomalous_ports_for_sensor(
                        sequence_id, cycle_id, sensor_num, cleaned_data_dir
                    )
                    if ports:
                        sensor_ports[sensor_key] = ports
        
        # Format port information string
        port_info_parts = []
        for sensor_key, ports in sensor_ports.items():
            ports_str = ",".join([f"P{p}" for p in ports])
            port_info_parts.append(f"{sensor_key} @ {ports_str}")
        
        port_info = "; ".join(port_info_parts) if port_info_parts else ""
        port_info_list.append(port_info)
    
    # Add port_info column
    results = results.with_columns([
        pl.Series("port_info", port_info_list)
    ])
    
    # Save enhanced results
    output_path.parent.mkdir(parents=True, exist_ok=True)
    results.write_csv(output_path)
    
    print(f"\nSaved enhanced results to: {output_path}")
    print(f"Added port information to {len(results):,} anomalies")


def main():
    parser = argparse.ArgumentParser(
        description='Add port information to anomaly detection results',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--results', '-r',
        required=True,
        help='Path to anomaly results CSV file'
    )
    
    parser.add_argument(
        '--cleaned-data', '-c',
        required=True,
        help='Path to cleaned data directory'
    )
    
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Output path for enhanced results CSV'
    )
    
    args = parser.parse_args()
    
    try:
        results_path = Path(args.results)
        cleaned_data_dir = Path(args.cleaned_data)
        output_path = Path(args.output)
        
        if not results_path.exists():
            raise FileNotFoundError(f"Results file does not exist: {results_path}")
        
        if not cleaned_data_dir.exists():
            raise FileNotFoundError(f"Cleaned data directory does not exist: {cleaned_data_dir}")
        
        add_port_info_to_anomalies(results_path, cleaned_data_dir, output_path)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())


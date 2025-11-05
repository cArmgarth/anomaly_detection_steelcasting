"""
Utility functions for creating metadata files for partitioned datasets.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import List


def create_parser_metadata(output_dir: Path, data_type: str, processed_dates: List[str], schema: dict = None):
    """
    Create metadata file for parser output directories (WistDB_files or OPCB).
    
    Args:
        output_dir: Directory where parsed files are stored
        data_type: Type of data (e.g., 'wistdb_bfl', 'opcb_castlength')
        processed_dates: List of dates that have been processed
        schema: Optional schema dictionary
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all existing parquet files in the directory
    existing_files = list(output_dir.glob("*.parquet"))
    existing_dates = sorted(set([f.stem.split('_')[0] for f in existing_files]))
    
    # Merge with provided dates
    all_dates = sorted(set(existing_dates + processed_dates))
    
    if not all_dates:
        return
    
    metadata = {
        "dataset_type": "partitioned_parquet",
        "data_type": data_type,
        "created_at": datetime.now().isoformat(),
        "processed_dates": all_dates,
        "total_dates": len(all_dates),
        "date_range": {
            "start": min(all_dates),
            "end": max(all_dates)
        },
        "file_pattern": f"{{date}}_{data_type}.parquet",
        "usage": {
            "polars": f"pl.scan_parquet('{output_dir}/*.parquet')",
            "pandas": f"pd.read_parquet('{output_dir}/', engine='pyarrow')",
            "pyarrow": f"pq.ParquetDataset('{output_dir}/')"
        }
    }
    
    if schema:
        metadata["schema"] = schema
    
    metadata_file = output_dir / "_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created metadata: {metadata_file}")


def update_parser_metadata(output_dir: Path, data_type: str, new_date: str):
    """
    Update existing metadata file with a new date.
    Tracks all dates across all parser types in the directory.
    
    Args:
        output_dir: Directory where parsed files are stored
        data_type: Type of data (informational, but not used for tracking)
        new_date: New date to add
    """
    metadata_file = output_dir / "_metadata.json"
    
    # Get all existing parquet files to determine all dates
    existing_files = list(output_dir.glob("*.parquet"))
    all_dates = sorted(set([f.stem.split('_')[0] for f in existing_files]))
    
    if metadata_file.exists():
        # Read existing metadata
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        # Merge dates from existing files and metadata
        existing_metadata_dates = set(metadata.get('processed_dates', []))
        all_dates_set = set(all_dates)
        combined_dates = sorted(list(existing_metadata_dates.union(all_dates_set)))
        
        # Update metadata
        metadata['processed_dates'] = combined_dates
        metadata['total_dates'] = len(combined_dates)
        if combined_dates:
            metadata['date_range'] = {
                "start": min(combined_dates),
                "end": max(combined_dates)
            }
        metadata['updated_at'] = datetime.now().isoformat()
        metadata['file_count'] = len(existing_files)
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    else:
        # Create new metadata - track all dates from existing files
        metadata = {
            "dataset_type": "partitioned_parquet",
            "directory": str(output_dir),
            "created_at": datetime.now().isoformat(),
            "processed_dates": all_dates,
            "total_dates": len(all_dates),
            "file_count": len(existing_files),
            "date_range": {
                "start": min(all_dates) if all_dates else None,
                "end": max(all_dates) if all_dates else None
            },
            "usage": {
                "polars": f"pl.scan_parquet('{output_dir}/*.parquet')",
                "pandas": f"pd.read_parquet('{output_dir}/', engine='pyarrow')",
                "pyarrow": f"pq.ParquetDataset('{output_dir}/')"
            }
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Created metadata: {metadata_file}")


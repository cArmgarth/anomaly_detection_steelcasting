#!/usr/bin/env python3
"""
Data Cleaning Script - Extract Casting Runs

Filters synced parquet data to extract only "run" data:
- A run starts when sequence_id appears (not 0 or null)
- A run ends when end_of_cast == 1 for that sequence_id
- Keeps all rows from first sequence_id occurrence up to and including first end_of_cast == 1
"""

import polars as pl
import argparse
import logging
import json
from pathlib import Path
from typing import Optional, List
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def filter_runs(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter data to extract only casting runs.
    
    Args:
        df: Polars LazyFrame with synced data
        
    Returns:
        Filtered LazyFrame with only run data
    """
    logger.info("Filtering runs from data...")
    
    # Step 1: Filter out rows where sequence_id is null or 0
    runs_df = (
        df
        .filter(
            (pl.col("sequence_id").is_not_null()) &
            (pl.col("sequence_id") != 0)
        )
        .sort("datetime")
    )
    
    # Step 2: For each sequence_id, find first end_of_cast == 1
    # and keep all rows up to and including that row
    runs_df = (
        runs_df
        .with_columns([
            # Find the datetime of first end_of_cast==1 for each sequence_id
            (
                pl.when(pl.col("end_of_cast") == 1)
                .then(pl.col("datetime"))
                .otherwise(None)
                .min()
                .over("sequence_id")
                .alias("first_eoc_dt")
            )
        ])
        # Keep only rows whose datetime is before or equal to first_eoc_dt
        .filter(pl.col("datetime") <= pl.col("first_eoc_dt"))
        # Drop helper column
        .drop("first_eoc_dt")
    )
    
    return runs_df


def create_partitioned_dataset_metadata(output_dir: Path, processed_dates: list):
    """Create metadata file for the partitioned cleaned dataset."""
    metadata = {
        "dataset_type": "partitioned_parquet",
        "data_type": "cleaned_casting_runs",
        "created_at": datetime.now().isoformat(),
        "processed_dates": processed_dates,
        "total_dates": len(processed_dates),
        "date_range": {
            "start": min(processed_dates),
            "end": max(processed_dates)
        },
        "description": "Cleaned casting run data - filtered to extract only run data (from sequence_id start to end_of_cast==1)",
        "usage": {
            "polars": "pl.scan_parquet('data_cleaning/cleaned_data/*.parquet')",
            "pandas": "pd.read_parquet('data_cleaning/cleaned_data/', engine='pyarrow')",
            "pyarrow": "pq.ParquetDataset('data_cleaning/cleaned_data/')"
        }
    }
    
    metadata_file = output_dir / "_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    logger.info(f"Created partitioned dataset metadata: {metadata_file}")


def clean_data(
    input_path: Path,
    output_path: Path,
    collect: bool = True
) -> Optional[pl.DataFrame]:
    """
    Clean data by extracting casting runs.
    
    Args:
        input_path: Path to input parquet file or directory
        output_path: Path to output parquet file or directory
        collect: Whether to collect the LazyFrame (default: True)
        
    Returns:
        DataFrame if collect=True, else None
    """
    logger.info(f"Loading data from {input_path}")
    
    # Load data
    if input_path.is_file() and input_path.suffix == '.parquet':
        df = pl.scan_parquet(input_path)
        input_is_file = True
        input_filename = input_path.stem
    elif input_path.is_dir():
        # Load all parquet files in directory
        parquet_files = sorted(input_path.glob('*.parquet'))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {input_path}")
        logger.info(f"Found {len(parquet_files)} parquet files")
        df = pl.scan_parquet(parquet_files)
        input_is_file = False
    else:
        raise FileNotFoundError(f"Path {input_path} is neither a parquet file nor a directory")
    
    # Get initial row count
    initial_count = df.select(pl.len()).collect().item()
    logger.info(f"Initial data: {initial_count:,} rows")
    
    # Filter runs
    cleaned_df = filter_runs(df)
    
    # Get cleaned row count
    cleaned_count = cleaned_df.select(pl.len()).collect().item()
    logger.info(f"Cleaned data: {cleaned_count:,} rows ({cleaned_count/initial_count*100:.1f}% retained)")
    
    # Count unique runs
    unique_runs = cleaned_df.select(pl.col("sequence_id").n_unique()).collect().item()
    logger.info(f"Unique casting runs: {unique_runs}")
    
    # Prepare output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if collect:
        # Collect and save
        logger.info("Collecting data...")
        result_df = cleaned_df.collect()
        
        if input_is_file:
            # Single file input: save to directory structure as partitioned dataset
            # Determine output directory
            if output_path.is_dir() or (output_path.suffix != '.parquet' and not output_path.exists()):
                # Output is a directory
                output_dir = Path(output_path)
                output_dir.mkdir(parents=True, exist_ok=True)
            else:
                # Output is a file path, use its parent directory
                output_dir = output_path.parent
                output_dir.mkdir(parents=True, exist_ok=True)
            
            # Extract date from filename or use date column
            if 'date' in result_df.columns:
                # Group by date and save each date as separate file
                processed_dates = []
                for date_val in result_df['date'].unique():
                    date_df = result_df.filter(pl.col('date') == date_val)
                    date_str = str(date_val)
                    date_output = output_dir / f"{date_str}.parquet"
                    date_df.write_parquet(date_output)
                    processed_dates.append(date_str)
                    logger.info(f"Saved {date_str}: {len(date_df):,} rows")
                
                # Create/update metadata for partitioned dataset
                if processed_dates:
                    # Read existing metadata if it exists
                    existing_metadata_file = output_dir / "_metadata.json"
                    existing_dates = []
                    if existing_metadata_file.exists():
                        with open(existing_metadata_file, 'r') as f:
                            existing_metadata = json.load(f)
                            existing_dates = existing_metadata.get('processed_dates', [])
                    
                    # Merge dates (avoid duplicates)
                    all_dates = sorted(list(set(existing_dates + processed_dates)))
                    create_partitioned_dataset_metadata(output_dir, all_dates)
            else:
                # Try to extract date from input filename
                date_str = input_path.stem
                # Save as date-based filename
                date_output = output_dir / f"{date_str}.parquet"
                result_df.write_parquet(date_output)
                logger.info(f"Saved cleaned data to {date_output}")
                
                # Create/update metadata for partitioned dataset
                existing_metadata_file = output_dir / "_metadata.json"
                existing_dates = []
                if existing_metadata_file.exists():
                    with open(existing_metadata_file, 'r') as f:
                        existing_metadata = json.load(f)
                        existing_dates = existing_metadata.get('processed_dates', [])
                
                # Merge dates (avoid duplicates)
                all_dates = sorted(list(set(existing_dates + [date_str])))
                create_partitioned_dataset_metadata(output_dir, all_dates)
        else:
            # Multiple files: save as directory with individual files per date
            if output_path.is_dir() or str(output_path).endswith('/'):
                # Save as directory with individual files
                output_path.mkdir(parents=True, exist_ok=True)
                # Group by date if date column exists
                processed_dates = []
                if 'date' in result_df.columns:
                    for date_val in result_df['date'].unique():
                        date_df = result_df.filter(pl.col('date') == date_val)
                        date_str = str(date_val)
                        date_output = output_path / f"{date_str}.parquet"
                        date_df.write_parquet(date_output)
                        processed_dates.append(date_str)
                        logger.info(f"Saved {date_str}: {len(date_df):,} rows")
                else:
                    # Save as single file if no date column
                    single_output = output_path / "cleaned_data.parquet"
                    result_df.write_parquet(single_output)
                    logger.info(f"Saved cleaned data to {single_output}")
                
                # Create metadata for partitioned dataset
                if processed_dates:
                    create_partitioned_dataset_metadata(output_path, processed_dates)
            else:
                # Single output file (not recommended for partitioned dataset)
                result_df.write_parquet(output_path)
                logger.info(f"Saved cleaned data to {output_path}")
        
        return result_df
    else:
        # Save as lazy (not implemented for now)
        logger.warning("Lazy saving not fully implemented, collecting data...")
        result_df = cleaned_df.collect()
        result_df.write_parquet(output_path)
        return None


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description='Clean synced data by extracting casting runs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Clean single parquet file
  python data_cleaning/cleaning.py --input parsed_files/synced/2025-09-02.parquet --output data_cleaning/cleaned_data/2025-09-02.parquet
  
  # Clean directory of parquet files
  python data_cleaning/cleaning.py --input parsed_files/synced/ --output data_cleaning/cleaned_data/
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input parquet file or directory'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='data_cleaning/cleaned_data/',
        help='Output parquet file or directory (default: data_cleaning/cleaned_data/)'
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
        
        # Clean data
        result_df = clean_data(input_path, output_path)
        
        print(f"\n=== Cleaning Summary ===")
        print(f"Input: {input_path}")
        print(f"Output: {output_path}")
        if result_df is not None:
            print(f"Rows: {len(result_df):,}")
            print(f"Columns: {len(result_df.columns)}")
            if 'sequence_id' in result_df.columns:
                print(f"Unique runs: {result_df['sequence_id'].n_unique()}")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())


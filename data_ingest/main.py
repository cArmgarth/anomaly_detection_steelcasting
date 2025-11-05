#!/usr/bin/env python3
"""
SSAB Data Ingestion Pipeline - Main Orchestrator

This script runs the complete data ingestion pipeline, parsing all data sources
and creating synchronized datasets for analysis.

Usage:
    python main.py                    # Process all available dates
    python main.py --date 2025-09-02  # Process specific date
    python main.py --dates 2025-09-01,2025-09-02  # Process multiple dates
    python main.py --help             # Show help
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path
from typing import List, Optional
import glob


def find_available_dates() -> List[str]:
    """Find all available dates from raw data files."""
    raw_data_dir = Path("raw_data")
    if not raw_data_dir.exists():
        print("ERROR: raw_data directory not found")
        return []
    
    # Look for WistDB files to determine available dates
    wistdb_files = list(raw_data_dir.glob("*.WistDB"))
    dates = []
    
    for file in wistdb_files:
        # Extract date from filename (e.g., "2025-09-02.WistDB" -> "2025-09-02")
        date_str = file.stem
        if date_str not in dates:
            dates.append(date_str)
    
    return sorted(dates)


def check_required_files(date: str) -> dict:
    """Check which files exist for a given date."""
    raw_data_dir = Path("raw_data")
    
    files = {
        'wistdb': raw_data_dir / f"{date}.WistDB",
        'opcb': raw_data_dir / f"{date}.OPCB",
        'wistsp': list(raw_data_dir.glob(f"{date}*.WistSP")),
        'wistix': raw_data_dir / f"{date}.WistIX",
        'pred': raw_data_dir / f"{date}.pred"
    }
    
    # Check existence
    status = {}
    status['wistdb'] = files['wistdb'].exists()
    status['opcb'] = files['opcb'].exists()
    status['wistsp'] = len(files['wistsp']) > 0
    status['wistix'] = files['wistix'].exists()
    status['pred'] = files['pred'].exists()
    
    return status


def run_parser(parser_script: str, input_file: str, description: str) -> bool:
    """Run a parser script and return success status."""
    try:
        print(f"  {description}...")
        result = subprocess.run([
            sys.executable, parser_script, input_file
        ], capture_output=True, text=True, check=True)
        
        print(f"    [OK] {description} completed")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"    [ERROR] {description} failed: {e}")
        if e.stdout:
            print(f"    stdout: {e.stdout}")
        if e.stderr:
            print(f"    stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"    [ERROR] {description} failed: {e}")
        return False


def process_date(date: str, skip_existing: bool = False) -> bool:
    """Process all parsers for a specific date."""
    print(f"\nProcessing {date}...")
    
    # Check if files exist
    file_status = check_required_files(date)
    
    if not file_status['wistdb']:
        print(f"  WARNING: WistDB file not found for {date}")
        return False
    
    if not file_status['opcb']:
        print(f"  WARNING: OPCB file not found for {date}")
        return False
    
    # Check if output already exists (partitioned dataset)
    if skip_existing:
        synced_file = Path(f"parsed_files/synced/{date}.parquet")
        if synced_file.exists():
            print(f"  Skipping {date} - partitioned file already exists")
            return True
    
    print(f"  Files found:")
    # Essential files
    essential_files = ['wistdb', 'opcb']
    for file_type in essential_files:
        status = "[OK]" if file_status[file_type] else "[MISSING]"
        print(f"    {status} {file_type.upper()}")
    
    # Optional files (informational only)
    optional_files = ['wistsp', 'wistix', 'pred']
    for file_type in optional_files:
        status = "[OK]" if file_status[file_type] else "[OPTIONAL]"
        print(f"    {status} {file_type.upper()}")
    
    # Run parsers
    parsers = [
        ("read_binary/WistDB_parser_working.py", f"raw_data/{date}.WistDB", "WistDB thermal data"),
        ("read_binary/castlength_parser.py", f"raw_data/{date}.OPCB", "Cast length data"),
        ("read_binary/endofcast_parser.py", f"raw_data/{date}.OPCB", "End of cast data"),
        ("read_binary/sequence_parser.py", f"raw_data/{date}.OPCB", "Sequence data"),
        ("read_binary/slabs_grade_parser.py", f"raw_data/{date}.OPCB", "Slabs grade data"),
        ("read_binary/rec_casting_speed_parser.py", f"raw_data/{date}.OPCB", "Rec casting speed data"),
        ("read_binary/width_pv_parser.py", f"raw_data/{date}.OPCB", "Width (pv) data"),
        ("read_binary/deltatloose_parser.py", f"raw_data/{date}.OPCB", "DeltaTLoose data"),
        ("read_binary/ar_to_sen_flow_parser.py", f"raw_data/{date}.OPCB", "Ar to SEN - flow data"),
        ("read_binary/ar_to_sen_pressure_parser.py", f"raw_data/{date}.OPCB", "Ar to SEN - pressure data"),
        ("read_binary/casting_program_parser.py", f"raw_data/{date}.OPCB", "Casting program data"),
        ("read_binary/tundish_pos_rel_parser.py", f"raw_data/{date}.OPCB", "Tundish pos (rel) data"),
        ("read_binary/mould_level_parser.py", f"raw_data/{date}.OPCB", "Mould level data")
    ]
    
    success_count = 0
    for parser_script, input_file, description in parsers:
        if run_parser(parser_script, input_file, description):
            success_count += 1
    
    print(f"  Parsers completed: {success_count}/{len(parsers)}")
    
    # Run syncer for this specific date
    print(f"  Synchronizing data...")
    try:
        result = subprocess.run([
            sys.executable, "read_binary/syncer.py", "--date", date
        ], capture_output=True, text=True, check=True)
        
        print(f"    [OK] Synchronization completed")
    except subprocess.CalledProcessError as e:
        print(f"    [ERROR] Synchronization failed: {e}")
        if e.stdout:
            print(f"    stdout: {e.stdout}")
        if e.stderr:
            print(f"    stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"    [ERROR] Synchronization failed: {e}")
        return False
    
    # Run cleaning step
    print(f"  Cleaning data (extracting casting runs)...")
    try:
        from pathlib import Path
        from data_cleaning.cleaning import clean_data
        
        synced_file = Path(f"parsed_files/synced/{date}.parquet")
        cleaned_dir = Path("data_cleaning/cleaned_data")
        
        if synced_file.exists():
            # Pass directory to ensure partitioned dataset structure
            clean_data(synced_file, cleaned_dir)
            print(f"    [OK] Cleaning completed")
        else:
            print(f"    [WARNING] Synced file not found, skipping cleaning")
        
        return True
        
    except Exception as e:
        print(f"    [ERROR] Cleaning failed: {e}")
        # Don't fail the entire pipeline if cleaning fails
        import traceback
        traceback.print_exc()
        return True  # Still return True since sync succeeded


def main():
    """Main pipeline orchestrator."""
    parser = argparse.ArgumentParser(
        description="SSAB Data Ingestion Pipeline - Complete data processing and synchronization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                           # Process all available dates
  python main.py --date 2025-09-02         # Process specific date
  python main.py --dates 2025-09-01,2025-09-02  # Process multiple dates
  python main.py --skip-existing           # Skip dates that already have synced files
  python main.py --list-dates              # List available dates and exit
        """
    )
    
    parser.add_argument(
        '--date', 
        help='Process specific date (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--dates', 
        help='Process multiple dates (comma-separated, YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--skip-existing', 
        action='store_true',
        help='Skip dates that already have synced output files'
    )
    parser.add_argument(
        '--list-dates', 
        action='store_true',
        help='List available dates and exit'
    )
    
    args = parser.parse_args()
    
    # List available dates if requested
    if args.list_dates:
        dates = find_available_dates()
        print("Available dates:")
        for date in dates:
            file_status = check_required_files(date)
            status = "[OK]" if file_status['wistdb'] and file_status['opcb'] else "[WARN]"
            print(f"  {status} {date}")
        return 0
    
    # Determine which dates to process
    if args.date:
        dates_to_process = [args.date]
    elif args.dates:
        dates_to_process = [d.strip() for d in args.dates.split(',')]
    else:
        dates_to_process = find_available_dates()
    
    if not dates_to_process:
        print("ERROR: No dates found to process")
        return 1
    
    print("SSAB Data Ingestion Pipeline")
    print("=" * 50)
    print(f"Processing {len(dates_to_process)} date(s): {', '.join(dates_to_process)}")
    
    if args.skip_existing:
        print("Skip existing files: ENABLED")
    
    # Process each date
    successful_dates = []
    failed_dates = []
    
    for date in dates_to_process:
        if process_date(date, args.skip_existing):
            successful_dates.append(date)
        else:
            failed_dates.append(date)
    
    print("\n" + "=" * 50)
    print("PIPELINE SUMMARY")
    print("=" * 50)
    print(f"Successful: {len(successful_dates)} date(s)")
    if successful_dates:
        for date in successful_dates:
            print(f"   * {date}")
    
    print(f"Failed: {len(failed_dates)} date(s)")
    if failed_dates:
        for date in failed_dates:
            print(f"   * {date}")
    
    # Check output files (partitioned dataset)
    synced_files = list(Path("parsed_files/synced").glob("*.parquet"))
    metadata_file = Path("parsed_files/synced/_metadata.json")
    
    if synced_files:
        print(f"\nPartitioned dataset files:")
        for file in synced_files:
            if file.name != "_metadata.json":
                print(f"   * {file}")
        if metadata_file.exists():
            print(f"   * {metadata_file} (metadata)")
    
    if failed_dates:
        return 1
    else:
        print(f"\nPipeline completed successfully!")
        return 0


if __name__ == "__main__":
    sys.exit(main())

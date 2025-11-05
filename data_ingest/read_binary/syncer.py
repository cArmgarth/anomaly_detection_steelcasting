"""
Data Synchronization Script

This script loads already parsed files and synchronizes them.
Creates partitioned Parquet datasets for better performance and scalability.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import glob
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def create_partitioned_dataset_metadata(output_dir: Path, processed_dates: list):
    """Create metadata file for the partitioned dataset."""
    metadata = {
        "dataset_type": "partitioned_parquet",
        "created_at": datetime.now().isoformat(),
        "processed_dates": processed_dates,
        "total_dates": len(processed_dates),
        "date_range": {
            "start": min(processed_dates),
            "end": max(processed_dates)
        },
        "schema": {
            "datetime": "timestamp[ns, UTC]",
            "mold_pos": "string",
            "port": "int64",
            "T_0": "float64",
            "T_1": "float64",
            # ... (other temperature columns)
            "cast_length_m": "float64",
            "end_of_cast": "float64",
            "sequence_id": "float64",
            "slabs_grade": "float64",
            "rec_casting_speed": "float64",
            "width_pv": "float64",
            "deltatloose": "float64",
            "ar_to_sen_flow": "float64",
            "ar_to_sen_pressure": "float64",
            "casting_program": "float64",
            "tundish_pos_rel": "float64",
            "mould_level": "float64",
            "date": "date32"
        },
        "usage": {
            "polars": "pl.scan_parquet('parsed_files/synced/*.parquet')",
            "pandas": "pd.read_parquet('parsed_files/synced/', engine='pyarrow')",
            "pyarrow": "pq.ParquetDataset('parsed_files/synced/')"
        }
    }
    
    import json
    metadata_file = output_dir / "_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Created dataset metadata: {metadata_file}")


def load_partitioned_data(target_dates=None, engine='polars'):
    """Load data from partitioned parquet dataset."""
    output_dir = Path("./parsed_files/synced")
    
    if engine == 'polars':
        import polars as pl
        if target_dates:
            if isinstance(target_dates, str):
                target_dates = [target_dates]
            # Filter specific dates
            files = [output_dir / f"{date}.parquet" for date in target_dates if (output_dir / f"{date}.parquet").exists()]
            if not files:
                return None
            return pl.concat([pl.read_parquet(f) for f in files])
        else:
            # Load all files
            return pl.scan_parquet(str(output_dir / "*.parquet")).collect()
    
    elif engine == 'pandas':
        if target_dates:
            if isinstance(target_dates, str):
                target_dates = [target_dates]
            files = [output_dir / f"{date}.parquet" for date in target_dates if (output_dir / f"{date}.parquet").exists()]
            if not files:
                return None
            return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        else:
            return pd.read_parquet(output_dir, engine='pyarrow')
    
    elif engine == 'pyarrow':
        dataset = pq.ParquetDataset(output_dir)
        if target_dates:
            if isinstance(target_dates, str):
                target_dates = [target_dates]
            # Filter by date partition
            filtered_files = [f for f in dataset.files if any(date in f for date in target_dates)]
            if not filtered_files:
                return None
            return pq.read_table(filtered_files).to_pandas()
        else:
            return dataset.read().to_pandas()
    
    else:
        raise ValueError("Engine must be 'polars', 'pandas', or 'pyarrow'")


def load_and_sync_data(target_dates=None):
    """Load parsed files and synchronize them."""
    
    # Find all available dates from parsed files
    wistdb_files = list(Path("./parsed_files/WistDB_files").glob("*_wistDB_bfl.parquet"))
    all_dates = []
    for file in wistdb_files:
        date_str = file.name.split('_')[0]  # Extract date from filename
        all_dates.append(date_str)
    
    # Filter to target dates if specified
    if target_dates is not None:
        if isinstance(target_dates, str):
            target_dates = [target_dates]
        dates = [d for d in all_dates if d in target_dates]
        print(f"Filtering to target dates: {target_dates}")
    else:
        dates = sorted(set(all_dates))
    
    print(f"Processing {len(dates)} dates: {dates}")
    
    # Create partitioned dataset directory
    output_dir = Path("./parsed_files/synced")
    output_dir.mkdir(exist_ok=True)
    
    # Process each date individually and save as separate parquet files
    processed_dates = []
    
    for date_str in dates:
        print(f"\nProcessing {date_str}...")
        
        # Load WistDB data (already BFL-only with required columns)
        wistdb_file = Path(f"./parsed_files/WistDB_files/{date_str}_wistDB_bfl.parquet")
        if not wistdb_file.exists():
            print(f"WARNING: WistDB file not found: {wistdb_file}")
            continue
            
        bfl_df = pd.read_parquet(wistdb_file, engine='pyarrow')
        bfl_df['datetime'] = pd.to_datetime(bfl_df['datetime'], utc=True, errors='coerce')
        
        # Remove rows with invalid timestamps
        bfl_df = bfl_df.dropna(subset=['datetime'])
        
        print(f"WistDB: {len(bfl_df)} BFL records")
        
        # Load OPCB data
        castlength_file = Path(f"./parsed_files/OPCB/{date_str}_castlength.parquet")
        endofcast_file = Path(f"./parsed_files/OPCB/{date_str}_endofcast.parquet")
        sequences_file = Path(f"./parsed_files/OPCB/{date_str}_sequences.parquet")
        slabs_grade_file = Path(f"./parsed_files/OPCB/{date_str}_slabs_grade.parquet")
        rec_casting_speed_file = Path(f"./parsed_files/OPCB/{date_str}_rec_casting_speed.parquet")
        width_pv_file = Path(f"./parsed_files/OPCB/{date_str}_width_pv.parquet")
        deltatloose_file = Path(f"./parsed_files/OPCB/{date_str}_deltatloose.parquet")
        ar_to_sen_flow_file = Path(f"./parsed_files/OPCB/{date_str}_ar_to_sen_flow.parquet")
        ar_to_sen_pressure_file = Path(f"./parsed_files/OPCB/{date_str}_ar_to_sen_pressure.parquet")
        casting_program_file = Path(f"./parsed_files/OPCB/{date_str}_casting_program.parquet")
        tundish_pos_rel_file = Path(f"./parsed_files/OPCB/{date_str}_tundish_pos_rel.parquet")
        mould_level_file = Path(f"./parsed_files/OPCB/{date_str}_mould_level.parquet")
        
        # Load castlength
        if castlength_file.exists():
            cl_df = pd.read_parquet(castlength_file, engine='pyarrow')
            cl_df['datetime'] = pd.to_datetime(cl_df['datetime'], utc=True, errors='coerce')
            cl_df = cl_df.dropna(subset=['datetime'])
            cl_df = cl_df[['datetime', 'cast_length_m']].copy()
            print(f"Cast Length: {len(cl_df)} records")
        else:
            cl_df = None
            print("Cast Length: No data")
        
        # Load endofcast
        if endofcast_file.exists():
            eoc_df = pd.read_parquet(endofcast_file, engine='pyarrow')
            eoc_df['datetime'] = pd.to_datetime(eoc_df['datetime'], utc=True, errors='coerce')
            eoc_df = eoc_df.dropna(subset=['datetime'])
            eoc_df = eoc_df[['datetime', 'end_of_cast']].copy()
            print(f"End of Cast: {len(eoc_df)} records")
        else:
            eoc_df = None
            print("End of Cast: No data")
        
        # Load sequences
        if sequences_file.exists():
            seq_df = pd.read_parquet(sequences_file, engine='pyarrow')
            seq_df['datetime'] = pd.to_datetime(seq_df['datetime'], utc=True, errors='coerce')
            seq_df = seq_df.dropna(subset=['datetime'])
            seq_df = seq_df[['datetime', 'sequence_id']].copy()
            print(f"Sequences: {len(seq_df)} records")
        else:
            seq_df = None
            print("Sequences: No data")
        
        # Load slabs grade
        if slabs_grade_file.exists():
            sg_df = pd.read_parquet(slabs_grade_file, engine='pyarrow')
            sg_df['datetime'] = pd.to_datetime(sg_df['datetime'], utc=True, errors='coerce')
            sg_df = sg_df.dropna(subset=['datetime'])
            sg_df = sg_df[['datetime', 'slabs_grade']].copy()
            print(f"Slabs Grade: {len(sg_df)} records")
        else:
            sg_df = None
            print("Slabs Grade: No data")
        
        # Load rec casting speed
        if rec_casting_speed_file.exists():
            rcs_df = pd.read_parquet(rec_casting_speed_file, engine='pyarrow')
            rcs_df['datetime'] = pd.to_datetime(rcs_df['datetime'], utc=True, errors='coerce')
            rcs_df = rcs_df.dropna(subset=['datetime'])
            rcs_df = rcs_df[['datetime', 'rec_casting_speed']].copy()
            print(f"Rec Casting Speed: {len(rcs_df)} records")
        else:
            rcs_df = None
            print("Rec Casting Speed: No data")
        
        # Load width (pv)
        if width_pv_file.exists():
            wpv_df = pd.read_parquet(width_pv_file, engine='pyarrow')
            wpv_df['datetime'] = pd.to_datetime(wpv_df['datetime'], utc=True, errors='coerce')
            wpv_df = wpv_df.dropna(subset=['datetime'])
            wpv_df = wpv_df[['datetime', 'width_pv']].copy()
            print(f"Width (pv): {len(wpv_df)} records")
        else:
            wpv_df = None
            print("Width (pv): No data")
        
        # Load deltatloose
        if deltatloose_file.exists():
            dtl_df = pd.read_parquet(deltatloose_file, engine='pyarrow')
            dtl_df['datetime'] = pd.to_datetime(dtl_df['datetime'], utc=True, errors='coerce')
            dtl_df = dtl_df.dropna(subset=['datetime'])
            dtl_df = dtl_df[['datetime', 'deltatloose']].copy()
            print(f"DeltaTLoose: {len(dtl_df)} records")
        else:
            dtl_df = None
            print("DeltaTLoose: No data")
        
        # Load Ar to SEN - flow
        if ar_to_sen_flow_file.exists():
            atsf_df = pd.read_parquet(ar_to_sen_flow_file, engine='pyarrow')
            atsf_df['datetime'] = pd.to_datetime(atsf_df['datetime'], utc=True, errors='coerce')
            atsf_df = atsf_df.dropna(subset=['datetime'])
            atsf_df = atsf_df[['datetime', 'ar_to_sen_flow']].copy()
            print(f"Ar to SEN - flow: {len(atsf_df)} records")
        else:
            atsf_df = None
            print("Ar to SEN - flow: No data")
        
        # Load Ar to SEN - pressure
        if ar_to_sen_pressure_file.exists():
            atsp_df = pd.read_parquet(ar_to_sen_pressure_file, engine='pyarrow')
            atsp_df['datetime'] = pd.to_datetime(atsp_df['datetime'], utc=True, errors='coerce')
            atsp_df = atsp_df.dropna(subset=['datetime'])
            atsp_df = atsp_df[['datetime', 'ar_to_sen_pressure']].copy()
            print(f"Ar to SEN - pressure: {len(atsp_df)} records")
        else:
            atsp_df = None
            print("Ar to SEN - pressure: No data")
        
        # Load casting program
        if casting_program_file.exists():
            cp_df = pd.read_parquet(casting_program_file, engine='pyarrow')
            cp_df['datetime'] = pd.to_datetime(cp_df['datetime'], utc=True, errors='coerce')
            cp_df = cp_df.dropna(subset=['datetime'])
            cp_df = cp_df[['datetime', 'casting_program']].copy()
            print(f"Casting Program: {len(cp_df)} records")
        else:
            cp_df = None
            print("Casting Program: No data")
        
        # Load tundish pos (rel)
        if tundish_pos_rel_file.exists():
            tpr_df = pd.read_parquet(tundish_pos_rel_file, engine='pyarrow')
            tpr_df['datetime'] = pd.to_datetime(tpr_df['datetime'], utc=True, errors='coerce')
            tpr_df = tpr_df.dropna(subset=['datetime'])
            tpr_df = tpr_df[['datetime', 'tundish_pos_rel']].copy()
            print(f"Tundish pos (rel): {len(tpr_df)} records")
        else:
            tpr_df = None
            print("Tundish pos (rel): No data")
        
        # Load mould level
        if mould_level_file.exists():
            ml_df = pd.read_parquet(mould_level_file, engine='pyarrow')
            ml_df['datetime'] = pd.to_datetime(ml_df['datetime'], utc=True, errors='coerce')
            ml_df = ml_df.dropna(subset=['datetime'])
            ml_df = ml_df[['datetime', 'mould_level']].copy()
            print(f"Mould level: {len(ml_df)} records")
        else:
            ml_df = None
            print("Mould level: No data")
        
        # Synchronize data for this date
        synced_df = bfl_df.copy()
        
        # Parameters that need interpolation (continuous values)
        interpolate_params = [
            ('cast_length_m', cl_df),
            ('rec_casting_speed', rcs_df),
            ('width_pv', wpv_df),
            ('deltatloose', dtl_df),
            ('ar_to_sen_flow', atsf_df),
            ('ar_to_sen_pressure', atsp_df),
            ('tundish_pos_rel', tpr_df),
            ('mould_level', ml_df)
        ]
        
        # Parameters that use backward merge (discrete/event-based values)
        merge_params = [
            ('end_of_cast', eoc_df),
            ('sequence_id', seq_df),
            ('slabs_grade', sg_df),
            ('casting_program', cp_df)
        ]
        
        # Process parameters that need interpolation
        # Sort synced_df by datetime once before the loop
        synced_df = synced_df.sort_values('datetime').reset_index(drop=True)
        
        for value_col, df in interpolate_params:
            if df is not None and not df.empty:
                # Sort OPCB dataframe by datetime and remove duplicates (keep last value)
                df_sorted = df[['datetime', value_col]].sort_values('datetime').copy()
                # Drop duplicate datetimes, keeping the last value
                df_sorted = df_sorted.drop_duplicates(subset=['datetime'], keep='last')
                
                # Ensure timezone consistency - make sure both are timezone-aware
                if synced_df['datetime'].dt.tz is None:
                    synced_df['datetime'] = pd.to_datetime(synced_df['datetime'], utc=True)
                if df_sorted['datetime'].dt.tz is None:
                    df_sorted['datetime'] = pd.to_datetime(df_sorted['datetime'], utc=True)
                
                # Create a Series with datetime index from source data
                # Drop NaN values from source data before interpolation
                df_sorted_clean = df_sorted.dropna(subset=[value_col])
                
                if df_sorted_clean.empty:
                    print(f"WARNING: {value_col} has no valid values, filling with NaN")
                    synced_df[value_col] = np.nan
                    continue
                
                source_series = df_sorted_clean.set_index('datetime')[value_col]
                # Ensure source_series has a DatetimeIndex
                source_series.index = pd.DatetimeIndex(source_series.index)
                
                # Get target timestamps from synced_df (already sorted) as DatetimeIndex
                target_timestamps = pd.DatetimeIndex(synced_df['datetime'])
                
                # Combine source and target timestamps, then sort and interpolate
                # This ensures we interpolate between actual source points, not just merge_asof results
                all_timestamps = pd.DatetimeIndex(
                    sorted(set(list(source_series.index) + list(target_timestamps)))
                )
                
                # Reindex source series to all timestamps (including target)
                # This creates NaNs for target timestamps that don't match source exactly
                reindexed_full = source_series.reindex(all_timestamps)
                
                # Interpolate using time-based interpolation on DatetimeIndex
                # This will interpolate between source points to fill in target timestamps
                try:
                    # Try time-based interpolation first (older pandas)
                    interpolated_full = reindexed_full.interpolate(method='time', limit_direction='both')
                except (ValueError, TypeError, AttributeError):
                    # For newer pandas versions, use index-based interpolation with DatetimeIndex
                    # This performs time-aware linear interpolation
                    interpolated_full = reindexed_full.interpolate(method='index', limit_direction='both')
                
                # Extract only the values for target timestamps
                interpolated_series = interpolated_full.reindex(target_timestamps)
                
                # Debug: Check if we got values
                overlap_count = interpolated_series.notna().sum()
                if overlap_count == 0 and len(source_series) > 0:
                    print(f"  DEBUG: Warning - no interpolated values for {value_col}")
                    print(f"    Source range: {source_series.index.min()} to {source_series.index.max()}")
                    print(f"    Target range: {target_timestamps.min()} to {target_timestamps.max()}")
                
                # For values outside the source range (before first or after last), use forward/backward fill
                # This ensures we have values even if target timestamps are outside source range
                interpolated_series = interpolated_series.ffill().bfill()
                
                # Assign interpolated values to synced_df (values are already in correct order)
                synced_df[value_col] = interpolated_series.values
                
                # Debug: check how many non-null values we got
                non_null_count = interpolated_series.notna().sum()
                print(f"Synced and interpolated {value_col} ({len(df_sorted)} source points -> {non_null_count}/{len(synced_df)} non-null interpolated points)")
            else:
                synced_df[value_col] = np.nan
                print(f"No data for {value_col}, filled with NaN")
        
        # Process parameters that use backward merge (discrete values)
        for value_col, df in merge_params:
            if df is not None and not df.empty:
                synced_df = pd.merge_asof(
                    synced_df.sort_values('datetime'),
                    df[['datetime', value_col]].sort_values('datetime'),
                    on='datetime',
                    direction='backward'
                )
                print(f"Synced {value_col}")
            else:
                synced_df[value_col] = np.nan
                print(f"No data for {value_col}, filled with NaN")
        
        # Add date partition column
        synced_df['date'] = synced_df['datetime'].dt.date
        
        # Save individual date as parquet file
        date_output_path = output_dir / f"{date_str}.parquet"
        synced_df.to_parquet(date_output_path, index=False, engine='pyarrow')
        
        processed_dates.append(date_str)
        print(f"SUCCESS: {date_str} synchronized - {len(synced_df)} records")
        print(f"Saved to: {date_output_path}")
    
    if not processed_dates:
        print("ERROR: No data successfully processed")
        return None
    
    # Create partitioned dataset metadata
    create_partitioned_dataset_metadata(output_dir, processed_dates)
    
    print(f"\nSUCCESS!")
    print(f"Processed {len(processed_dates)} dates: {processed_dates}")
    print(f"Partitioned dataset created in: {output_dir}")
    
    return processed_dates

if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description='Synchronize parsed data into partitioned parquet dataset')
    parser.add_argument('--date', help='Process specific date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    if args.date:
        processed_dates = load_and_sync_data([args.date])
    else:
        processed_dates = load_and_sync_data()
    
    if processed_dates is not None:
        print(f"\nPartitioned Dataset Summary:")
        print(f"Processed dates: {processed_dates}")
        print(f"Total dates: {len(processed_dates)}")
        print(f"Dataset location: parsed_files/synced/")
        print(f"\nTo load data:")
        print(f"  Polars: pl.scan_parquet('parsed_files/synced/*.parquet').collect()")
        print(f"  Pandas: pd.read_parquet('parsed_files/synced/', engine='pyarrow')")
        print(f"  PyArrow: pq.ParquetDataset('parsed_files/synced/')")

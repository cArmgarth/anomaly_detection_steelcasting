"""
WistDB Parser - Full Resolution Focus BFL Only

This parser focuses on full resolution output for BFL only.
"""

import struct
import pandas as pd
from datetime import datetime, timezone
import os
import sys


class WistDBParser:
    """Simple parser for .WistDB files focused on full resolution output."""
    
    # LabVIEW epoch: January 1, 1904 00:00:00.00 UTC
    LABVIEW_EPOCH = datetime(1904, 1, 1, 0, 0, 0, 0, timezone.utc)
    
    def __init__(self):
        """Initialize parser."""
        pass
    
    def _get_output_filename(self, filepath: str, suffix: str, with_extension: bool = True):
        """
        Generate output filename in parsed_files folder based on input file date.
        
        Args:
            filepath: Input .WistDB file path
            suffix: Suffix to add (e.g., 'full_resolution')
            with_extension: Whether to include .csv extension
            
        Returns:
            Output file path in parsed_files folder
        """
        # Extract filename without path
        filename = os.path.basename(filepath)
        
        # Extract date from filename (e.g., "2025-05-12.WistDB" -> "2025-05-12")
        date_part = os.path.splitext(filename)[0]
        
        # Create parsed_files directory if it doesn't exist
        parsed_dir = "./parsed_files/WistDB_files"
        os.makedirs(parsed_dir, exist_ok=True)
        
        # Generate output filename
        if with_extension:
            output_filename = f"{date_part}_{suffix}.csv"
        else:
            output_filename = f"{date_part}_{suffix}"
        
        return os.path.join(parsed_dir, output_filename)
    
    def _read_single_record(self, f):
        """Read one record from the file with improved error handling."""
        try:
            # Read 6 elements as specified in the LabVIEW structure
            
            # 1. Absolute time (DBL, 8 bytes) - little endian
            time_bytes = f.read(8)
            if len(time_bytes) < 8:
                return None
            absolute_time = struct.unpack('<d', time_bytes)[0]
            
            # Validate timestamp (reasonable bounds)
            if absolute_time < 0 or absolute_time > 1e10:
                return None
            
            # 2. Fiber Pos (SGL, 4 bytes) - ignored but read
            fiber_pos = struct.unpack('<f', f.read(4))[0]
            
            # 3. port (UB, 1 byte)
            port = struct.unpack('<B', f.read(1))[0]
            
            # 4. Wistom (UB, 1 byte)
            wistom = struct.unpack('<B', f.read(1))[0]
            
            # 5. Temperature array: U32 length + U16[] values
            # Read length first
            length_bytes = f.read(4)
            if len(length_bytes) < 4:
                return None
            temp_length = struct.unpack('<I', length_bytes)[0]
            
            # Read temperature values
            temps = []
            if temp_length > 0:
                temp_bytes = f.read(temp_length * 2)
                if len(temp_bytes) < temp_length * 2:
                    return None
                temps = list(struct.unpack(f'<{temp_length}H', temp_bytes))
            
            # 6. Average amplitude (SGL, 4 bytes)
            avg_amplitude = struct.unpack('<f', f.read(4))[0]
            
            return {
                'labview_time': absolute_time,
                'fiber_pos': fiber_pos,
                'port': port,
                'wistom': wistom,
                'num_temps': temp_length,
                'temperatures': temps,
                'avg_amplitude': avg_amplitude
            }
            
        except Exception as e:
            return None
    
    def _convert_time(self, labview_time):
        """Convert LabVIEW timestamp to UTC time with proper error handling."""
        try:
            # Validate input
            if labview_time is None or pd.isna(labview_time):
                return None
            
            # Check for reasonable timestamp values (not too far in past/future)
            if labview_time < 0 or labview_time > 1e10:  # Reasonable bounds
                return None
            
            # Convert to UTC datetime
            utc_time = self.LABVIEW_EPOCH + pd.Timedelta(seconds=labview_time)
            
            # Shift forward 2 hours to align with midnight-to-midnight timeframe
            utc_time = utc_time + pd.Timedelta(hours=2)
            
            # Ensure timezone awareness
            if utc_time.tzinfo is None:
                utc_time = utc_time.replace(tzinfo=timezone.utc)
            
            return utc_time
            
        except (ValueError, OverflowError, TypeError) as e:
            # Log the specific error for debugging
            print(f"Warning: Failed to convert timestamp {labview_time}: {e}")
            return None
        except Exception as e:
            # Catch any other unexpected errors
            print(f"Warning: Unexpected error converting timestamp {labview_time}: {e}")
            return None
    
    def parse_file_all_records(self, filepath: str):
        """
        Parse ALL records from a .WistDB file without any sampling.
        
        Args:
            filepath: Path to the .WistDB file
            
        Returns:
            DataFrame with all records
        """
        print(f"Parsing ALL records from {filepath} (no sampling)...")
        
        records = []
        record_count = 0
        
        with open(filepath, 'rb') as f:
            while True:
                try:
                    record = self._read_single_record(f)
                    if record is None:
                        break
                    
                    records.append(record)
                    record_count += 1
                    
                    if record_count % 10000 == 0:
                        print(f"Parsed {record_count:,} records...")
                        
                except Exception as e:
                    print(f"Error at record {record_count}: {e}")
                    break
        
        print(f"Successfully parsed {len(records):,} records")
        
        if records:
            # Create DataFrame
            df = pd.DataFrame(records)
            
            # Convert timestamps with better error handling
            df['datetime'] = df['labview_time'].apply(self._convert_time)
            
            # Remove rows with invalid timestamps
            initial_count = len(df)
            df = df.dropna(subset=['datetime'])
            removed_count = initial_count - len(df)
            
            if removed_count > 0:
                print(f"Removed {removed_count} records with invalid timestamps")
            
            # Ensure timezone awareness and proper datetime formatting
            if not df.empty:
                df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
            
            # Basic info
            print(f"Time range: {df['datetime'].min()} to {df['datetime'].max()}")
            print(f"Ports: {sorted(df['port'].unique())}")
            print(f"Temperature array sizes: {df['num_temps'].value_counts().head()}")
            
            return df
        
        return pd.DataFrame()
    
    def create_full_resolution_analysis(self, filepath: str):
        """
        Create analysis DataFrame from entire file with full 1-second resolution (no sampling).
        
        Args:
            filepath: Path to the .WistDB file
            
        Returns:
            DataFrame with expanded temperature data for full analysis
        """
        print(f"Creating full resolution analysis from {filepath}...")
        print("Processing ALL records with temperature expansion...")
        
        # Parse ALL records without sampling
        df = self.parse_file_all_records(filepath)
        
        if df.empty:
            return pd.DataFrame()
        
        # Filter to records with temperature data
        temp_df = df[df['num_temps'] > 0].copy()
        
        if temp_df.empty:
            print("No temperature data found")
            return df
        
        # Create expanded temperature columns for the most common sensor count
        most_common_sensors = temp_df['num_temps'].mode().iloc[0]
        print(f"Expanding temperature data for {most_common_sensors} sensors...")
        
        # Filter to records with the most common sensor count
        analysis_df = temp_df[temp_df['num_temps'] == most_common_sensors].copy()
        
        # Expand temperature arrays into individual columns
        temp_columns = {}
        for i in range(most_common_sensors):
            temp_columns[f'T_{i}'] = [temps[i] * 0.01 if i < len(temps) else None 
                                     for temps in analysis_df['temperatures']]
        
        # Add temperature columns to DataFrame
        for col_name, values in temp_columns.items():
            analysis_df[col_name] = values
        
        # Add mold position column
        def assign_mold_pos(port):
            if 1 <= port <= 16:
                return "BFF"
            elif 17 <= port <= 32:
                return "BFL"
            elif 33 <= port <= 36:
                return "NFR"
            elif 37 <= port <= 40:
                return "NFL"
            else:
                return None
        
        analysis_df["mold_pos"] = analysis_df["port"].apply(assign_mold_pos)
        
        # Filter to BFL only
        bfl_df = analysis_df[analysis_df['mold_pos'] == 'BFL'].copy()
        
        if bfl_df.empty:
            print("No BFL data found")
            return pd.DataFrame()
        
        print(f"Filtered to {len(bfl_df):,} BFL records")
        analysis_df = bfl_df
        
        # Drop the original temperatures column
        analysis_df = analysis_df.drop('temperatures', axis=1)
        
        # Ensure datetime column is properly formatted before saving
        analysis_df['datetime'] = pd.to_datetime(analysis_df['datetime'], utc=True)
        
        # Select only required columns: datetime, mold_pos, port, T_x...T_n
        required_cols = ['datetime', 'mold_pos', 'port']
        temp_cols = [col for col in analysis_df.columns if col.startswith('T_')]
        final_df = analysis_df[required_cols + temp_cols].copy()
        
        # Save to parsed_files folder with custom naming
        import os
        from pathlib import Path
        
        # Extract date from filename
        filename = os.path.basename(filepath)
        date_part = filename.split('.')[0]  # Get date part before extension
        
        # Create output directory if it doesn't exist
        output_dir = Path("parsed_files/WistDB_files")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create output filename: date_wistDB_bfl.csv
        output_file = output_dir / f"{date_part}_wistDB_bfl.csv"
        
        final_df.to_csv(output_file, index=False)
        print(f"BFL-only analysis saved to: {output_file}")
        print(f"DataFrame shape: {final_df.shape}")
        
        # Show time coverage
        print(f"Time coverage: {final_df['datetime'].min()} to {final_df['datetime'].max()}")
        duration = final_df['datetime'].max() - final_df['datetime'].min()
        print(f"Duration: {duration}")
        
        return final_df
    


def main():
    """Interface for BFL-only WistDB parsing."""
    parser = WistDBParser()
    
    # Get command line arguments
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python WistDB_parser_working.py <file>")
        print("")
        print("This will create a BFL-only analysis with temperature data.")
        return
    
    filepath = sys.argv[1]
    
    # Create full resolution analysis with temperature expansion
    df = parser.create_full_resolution_analysis(filepath)
    
    if not df.empty:
        print("\nFull resolution analysis created!")
        print(f"Columns: {list(df.columns)}")
        print(f"Shape: {df.shape}")
        print("\nFirst few rows:")
        print(df.head())


if __name__ == "__main__":
    main()
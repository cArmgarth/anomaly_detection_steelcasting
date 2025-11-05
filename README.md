# SSAB Data Ingestion System

A comprehensive Python system for parsing and synchronizing steel casting data from multiple sources including WistDB thermal measurements, OPCB process data, and other industrial sensors.

## 🏗️ System Architecture

```
data_ingest/
├── raw_data/              # Input data files (gitignored)
│   ├── *.WistDB          # Thermal measurement data
│   ├── *.OPCB            # Process control data
│   └── *.WistSP, *.WistIX, *.pred
├── parsed_files/          # Processed data output (gitignored)
│   ├── WistDB_files/     # Parsed thermal data (CSV)
│   │   └── *_wistDB_bfl.csv
│   ├── OPCB/             # Parsed process data (CSV)
│   │   ├── *_castlength.csv
│   │   ├── *_endofcast.csv
│   │   ├── *_sequences.csv
│   │   ├── *_slabs_grade.csv
│   │   ├── *_rec_casting_speed.csv
│   │   ├── *_width_pv.csv
│   │   ├── *_deltatloose.csv
│   │   ├── *_ar_to_sen_flow.csv
│   │   ├── *_ar_to_sen_pressure.csv
│   │   └── *_casting_program.csv
│   └── synced/           # Partitioned synchronized datasets (Parquet)
│       ├── YYYY-MM-DD.parquet
│       └── _metadata.json
├── data_exploration/      # Analysis notebooks
│   └── synced_exploration.ipynb
├── data_cleaning/        # Data cleaning and filtering
│   ├── cleaning.py      # Extract casting runs from synced data
│   └── cleaned_data/    # Cleaned datasets (Parquet, gitignored)
│       └── YYYY-MM-DD.parquet
├── calculations/         # Analysis and calculations (for future scripts)
├── read_binary/          # Parser modules
│   ├── WistDB_parser_working.py
│   ├── syncer.py
│   ├── castlength_parser.py
│   ├── endofcast_parser.py
│   ├── sequence_parser.py
│   ├── slabs_grade_parser.py
│   ├── rec_casting_speed_parser.py
│   ├── width_pv_parser.py
│   ├── deltatloose_parser.py
│   ├── ar_to_sen_flow_parser.py
│   ├── ar_to_sen_pressure_parser.py
│   ├── casting_program_parser.py
│   └── opcb_channels_report.md
├── main.py              # Main pipeline orchestrator
├── pyproject.toml        # Project dependencies (includes polars, pyarrow for Parquet)
├── uv.lock              # Dependency lock file
├── .python-version      # Python version specification
├── .gitignore           # Git ignore patterns
├── README.md            # This file
└── POLARS_QUICK_START.md # Quick start guide for Polars
```

## 🚀 Quick Start

### 1. Complete Pipeline (RECOMMENDED)

Use the main orchestrator script to process all data sources automatically:

```bash
# Process all available dates
uv run python main.py

# Process specific date
uv run python main.py --date 2025-09-02

# Process multiple dates
uv run python main.py --dates 2025-09-01,2025-09-02

# Skip dates that already have synced files
uv run python main.py --skip-existing

# List available dates
uv run python main.py --list-dates

# Show help
uv run python main.py --help
```

This will automatically:
- Parse all WistDB thermal data
- Extract OPCB process data (cast length, end of cast, sequences, slabs grade, casting speed, width, DeltaTLoose, Ar flow/pressure, casting program)
- Synchronize all data sources
- Create unified datasets ready for analysis

### 2. Individual Parsers

**WistDB Thermal Data:**
```bash
uv run python read_binary/WistDB_parser_working.py raw_data/2025-09-01.WistDB
```

**OPCB Process Data:**
```bash
# Cast length data
uv run python read_binary/castlength_parser.py raw_data/2025-09-01.OPCB

# End of cast events
uv run python read_binary/endofcast_parser.py raw_data/2025-09-01.OPCB

# Sequence information
uv run python read_binary/sequence_parser.py raw_data/2025-09-01.OPCB

# Slabs grade information
uv run python read_binary/slabs_grade_parser.py raw_data/2025-09-01.OPCB

# Rec casting speed
uv run python read_binary/rec_casting_speed_parser.py raw_data/2025-09-01.OPCB

# Width (pv)
uv run python read_binary/width_pv_parser.py raw_data/2025-09-01.OPCB

# DeltaTLoose
uv run python read_binary/deltatloose_parser.py raw_data/2025-09-01.OPCB

# Ar to SEN - flow
uv run python read_binary/ar_to_sen_flow_parser.py raw_data/2025-09-01.OPCB

# Ar to SEN - pressure
uv run python read_binary/ar_to_sen_pressure_parser.py raw_data/2025-09-01.OPCB

# Casting program
uv run python read_binary/casting_program_parser.py raw_data/2025-09-01.OPCB
```

### 2. Synchronize All Data (RECOMMENDED)

```bash
# Sync all available dates
uv run python data_ingest/read_binary/syncer.py

# Sync specific date
uv run python data_ingest/read_binary/syncer.py --date 2025-09-01
```

This will:
- Parse all available dates from raw data
- Synchronize WistDB thermal data with OPCB process data
- **Interpolate continuous parameters**: Uses time-based interpolation to fill all timestamps for continuous measurements (cast_length_m, rec_casting_speed, width_pv, deltatloose, ar_to_sen_flow, ar_to_sen_pressure, tundish_pos_rel)
- **Merge discrete events**: Uses backward merge for event-based data (end_of_cast, sequence_id, slabs_grade, casting_program)
- Create unified datasets with temperature sensors, cast length, end-of-cast events, sequence information, slabs grade, casting speed, width, DeltaTLoose, Ar flow/pressure, tundish position, and casting program data
- Save synchronized data to `data_ingest/parsed_files/synced/` as partitioned Parquet files (one file per date)

### 3. Clean Data (Extract Casting Runs)

```bash
# Clean all synced data
uv run python data_ingest/data_cleaning/cleaning.py --input data_ingest/parsed_files/synced/ --output data_ingest/data_cleaning/cleaned_data/

# Clean specific date
uv run python data_ingest/data_cleaning/cleaning.py --input data_ingest/parsed_files/synced/2025-09-01.parquet --output data_ingest/data_cleaning/cleaned_data/
```

This extracts only casting runs (removes idle time, setup periods):
- Filters data where `sequence_id` is not null and not 0
- Keeps rows from sequence start until first `end_of_cast == 1`
- Typically retains 34-40% of original data (only casting runs)

### 4. Analyze Data

```bash
# Open Jupyter notebooks for analysis
jupyter notebook data_ingest/data_exploration/synced_exploration.ipynb
jupyter notebook data_ingest/data_exploration/single_day_ex.ipynb
```

**Recommended: Use Polars for better performance**
```python
import polars as pl

# Load cleaned casting runs (recommended for analysis)
df = pl.scan_parquet('data_ingest/data_cleaning/cleaned_data/*.parquet').collect()

# Or load all synced data
df = pl.scan_parquet('data_ingest/parsed_files/synced/*.parquet').collect()

# Or load specific dates
df = pl.concat([
    pl.read_parquet('data_ingest/parsed_files/synced/2025-09-01.parquet'),
    pl.read_parquet('data_ingest/parsed_files/synced/2025-09-02.parquet')
])

# Convert to pandas only when needed
pandas_df = df.to_pandas()
```

## 📊 Data Sources

### WistDB Thermal Measurements
- **Source**: WistSense thermal measurement system
- **Data**: Temperature profiles across 71 sensors
- **Frequency**: ~1 second per port (40 ports total)
- **Coverage**: Full casting process thermal monitoring

### OPCB Process Control Data
- **Cast Length**: Continuous measurement of steel length
- **End of Cast**: Discrete events marking cast completion
- **Sequences**: Process sequence identification
- **Slabs Grade**: Steel grade identification for each casting sequence
- **Rec Casting Speed**: Recommended casting speed measurements
- **Width (pv)**: Width process variable measurements
- **DeltaTLoose**: Temperature delta loose measurements
- **Ar to SEN - flow**: Argon flow to Submerged Entry Nozzle
- **Ar to SEN - pressure**: Argon pressure to Submerged Entry Nozzle
- **Tundish Position (rel)**: Tundish position relative measurements
- **Casting Program**: Casting program identifier
- **Frequency**: Variable, synchronized to process events
- **Interpolation**: Continuous parameters are interpolated to match thermal measurement timestamps

## 🛠️ Parser Modules

### `main.py`
**Purpose**: Main pipeline orchestrator - runs the complete data ingestion pipeline
- Automatically discovers available dates from raw data files
- Runs all parsers in sequence for each date
- Synchronizes all data sources into unified datasets
- Provides flexible command-line options for date selection
- Handles error reporting and progress tracking
- Usage: `uv run python main.py [options]`

### `WistDB_parser_working.py`
**Purpose**: Parse thermal measurement data from .WistDB files
- Extracts temperature arrays from 71 sensors
- Converts timestamps to UTC
- Filters to BFL (Bottom of Furnace Level) data only
- Outputs: `parsed_files/WistDB_files/YYYY-MM-DD_wistDB_bfl.csv`

### `castlength_parser.py`
**Purpose**: Extract cast length measurements from .OPCB files
- Processes continuous length measurements
- Applies timezone offset (+2 hours)
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_castlength.csv`

### `endofcast_parser.py`
**Purpose**: Extract end-of-cast events from .OPCB files
- Identifies discrete cast completion events
- Binary flag (0/1) for each timestamp
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_endofcast.csv`

### `sequence_parser.py`
**Purpose**: Extract sequence information from .OPCB files
- Tracks process sequence IDs
- Links to casting process stages
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_sequences.csv`

### `slabs_grade_parser.py`
**Purpose**: Extract slabs grade information from .OPCB files
- Tracks steel grade identification for each casting sequence
- Links grade data to process timing
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_slabs_grade.csv`

### `rec_casting_speed_parser.py`
**Purpose**: Extract recommended casting speed from .OPCB files
- Extracts casting speed measurements
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_rec_casting_speed.csv`

### `width_pv_parser.py`
**Purpose**: Extract width process variable from .OPCB files
- Extracts width measurements (pv = process variable)
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_width_pv.csv`

### `deltatloose_parser.py`
**Purpose**: Extract DeltaTLoose measurements from .OPCB files
- Extracts temperature delta loose measurements
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_deltatloose.csv`

### `ar_to_sen_flow_parser.py`
**Purpose**: Extract Argon flow to SEN from .OPCB files
- Extracts Argon flow measurements to Submerged Entry Nozzle
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_ar_to_sen_flow.csv`

### `ar_to_sen_pressure_parser.py`
**Purpose**: Extract Argon pressure to SEN from .OPCB files
- Extracts Argon pressure measurements to Submerged Entry Nozzle
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_ar_to_sen_pressure.csv`

### `casting_program_parser.py`
**Purpose**: Extract casting program identifier from .OPCB files
- Extracts casting program identification
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_casting_program.csv`

### `tundish_pos_rel_parser.py`
**Purpose**: Extract tundish position relative from .OPCB files
- Extracts tundish position relative measurements
- Outputs: `parsed_files/OPCB/YYYY-MM-DD_tundish_pos_rel.csv`

### `syncer.py`
**Purpose**: Synchronize all data sources into unified datasets
- Merges thermal data with process data using time-based alignment
- **Time-based interpolation**: Intelligently interpolates continuous process parameters (cast_length_m, rec_casting_speed, width_pv, deltatloose, ar_to_sen_flow, ar_to_sen_pressure, tundish_pos_rel) between source timestamps to fill all target timestamps
- **Backward merge**: Uses backward merge for discrete/event-based values (end_of_cast, sequence_id, slabs_grade, casting_program)
- Creates comprehensive datasets for analysis with 100% non-null interpolated values
- Outputs efficient Parquet files for better performance and compression
- Creates partitioned dataset (one file per date) for scalability
- Handles multiple dates automatically
- Outputs: `parsed_files/synced/YYYY-MM-DD.parquet` (one file per date)
- Usage: `uv run python data_ingest/read_binary/syncer.py [--date YYYY-MM-DD]`

### `data_cleaning/cleaning.py`
**Purpose**: Extract casting runs from synchronized data
- Filters data to extract only "run" data (actual casting operations)
- Removes idle time, setup, and non-production periods
- Uses `sequence_id` and `end_of_cast` flags to identify runs
- Typically retains 34-40% of original data (only casting runs)
- Outputs: `data_ingest/data_cleaning/cleaned_data/YYYY-MM-DD.parquet`
- Usage: `uv run python data_ingest/data_cleaning/cleaning.py --input <input> --output <output>`

## 🎯 Why Polars?

We use **Polars** for data analysis because it offers significant advantages:

1. **Performance**: 2-5x faster than pandas for most operations
2. **Memory Efficiency**: Lower memory footprint for large datasets
3. **Lazy Evaluation**: Optimized queries that only process what's needed
4. **Native Parquet Support**: Direct, efficient Parquet file reading without conflicts
5. **Modern API**: Expressive and type-safe operations
6. **Pandas Compatibility**: Easy `.to_pandas()` conversion when needed

### Quick Comparison

| Feature | Pandas | Polars |
|---------|--------|--------|
| Loading speed | Fast | 2x faster |
| Memory usage | High | 30% lower |
| Large datasets | Slower | Optimized |
| Parquet support | Good | Native |
| Lazy evaluation | No | Yes |

### When to Use Each

**Use Polars for:**
- Loading and processing large Parquet files
- Complex aggregations and transformations
- Data filtering and joining
- Time-series analysis

**Convert to Pandas for:**
- Libraries that specifically require pandas
- Legacy code compatibility
- Plotting with pandas-compatible tools (though Polars has direct plot support too)

## 📊 Data Format Benefits

### Parquet Format for Synchronized Data

The synchronized datasets are saved in Parquet format for several advantages:

- **Compression**: ~75% smaller file sizes compared to CSV
- **Performance**: Faster read/write operations
- **Schema Preservation**: Maintains data types automatically
- **Columnar Storage**: Efficient for analytical queries
- **Cross-Platform**: Compatible with Python, R, Spark, and more

Example file sizes for 1M+ records:
- CSV: ~567 MB
- Parquet: ~139 MB (75% reduction)

### Partitioned Dataset Benefits

The synchronized data is stored as a **partitioned Parquet dataset** with several advantages:

- **Scalability**: One file per date - easy to add new dates without reprocessing
- **Query Performance**: Load only the dates you need
- **Parallel Processing**: Process multiple dates simultaneously
- **Incremental Updates**: Add new dates without touching existing data
- **Memory Efficiency**: Load only required partitions into memory

**Dataset Structure:**
```
parsed_files/synced/
├── 2025-09-01.parquet    # ~42 MB
├── 2025-09-02.parquet    # ~98 MB
├── 2025-09-03.parquet    # ~37 MB
└── _metadata.json        # Dataset metadata
```

### Example Polars Usage

```python
import polars as pl

# Load all partitioned files
df = pl.scan_parquet('parsed_files/synced/*.parquet').collect()

# Load specific dates only
df = pl.concat([
    pl.read_parquet('parsed_files/synced/2025-09-01.parquet'),
    pl.read_parquet('parsed_files/synced/2025-09-02.parquet')
])

# Filter by date partition
df = pl.scan_parquet('parsed_files/synced/*.parquet').filter(
    pl.col('date') == pl.date(2025, 9, 1)
).collect()

# Convert to pandas only when needed
pandas_df = df.to_pandas()
```

**📖 For more details, see the [Polars Quick Start Guide](POLARS_QUICK_START.md)**

## 📁 Output Data Structure

### Synchronized Dataset Columns
- `datetime`: UTC timestamp
- `mold_pos`: Mold position (BFL - Bottom of Furnace Level)
- `port`: Port number (1-40)
- `T_0` to `T_70`: Individual temperature sensors (°C)
- `cast_length_m`: Steel length measurement (m)
- `end_of_cast`: End-of-cast flag (0/1)
- `sequence_id`: Process sequence identifier
- `slabs_grade`: Steel grade identification for each casting sequence
- `rec_casting_speed`: Recommended casting speed
- `width_pv`: Width process variable
- `deltatloose`: Temperature delta loose
- `ar_to_sen_flow`: Argon flow to Submerged Entry Nozzle
- `ar_to_sen_pressure`: Argon pressure to Submerged Entry Nozzle
- `tundish_pos_rel`: Tundish position relative
- `casting_program`: Casting program identifier
- `date`: Date partition column (for partitioned dataset)

### Data Quality
- **Temporal alignment**: All data synchronized to common timestamps
- **Interpolation**: Continuous parameters (cast_length_m, rec_casting_speed, width_pv, deltatloose, ar_to_sen_flow, ar_to_sen_pressure, tundish_pos_rel) are interpolated using time-based interpolation to ensure 100% non-null values
- **Event merging**: Discrete events (end_of_cast, sequence_id, slabs_grade, casting_program) use backward merge to propagate values forward
- **Missing data**: Filled with NaN only where no source data exists
- **Time coverage**: Full 24-hour periods
- **Sensor count**: 71 temperature sensors per record

## 🔧 Usage Examples

### Process Single Date
```bash
# Sync all dates
uv run python data_ingest/read_binary/syncer.py

# Sync specific date
uv run python data_ingest/read_binary/syncer.py --date 2025-09-01
```

### Process Specific Dates (via Python)
```python
import sys
import os
# Add the read_binary directory to Python path
sys.path.append(os.path.join(os.getcwd(), 'read_binary'))
from syncer import load_and_sync_data

# Load specific date
df = load_and_sync_data(target_dates="2025-09-01")

# Load multiple dates
df = load_and_sync_data(target_dates=["2025-09-01", "2025-09-02"])
```

### Analyze Data with Polars (Recommended)
```python
import polars as pl

# Load partitioned synchronized data with Polars
df = pl.scan_parquet("parsed_files/synced/*.parquet").collect()

# Or load specific dates
df = pl.concat([
    pl.read_parquet("parsed_files/synced/2025-09-01.parquet"),
    pl.read_parquet("parsed_files/synced/2025-09-02.parquet")
])

# Basic analysis
print(f"Shape: {df.shape}")
print(f"Temperature sensors: {len([col for col in df.columns if col.startswith('T_')])}")
print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")

# Filter and aggregate
sept_2_data = df.filter(pl.col('date') == pl.date(2025, 9, 2))
temp_stats = df.select([col for col in df.columns if col.startswith('T_')]).describe()
print(temp_stats)
```

### Analyze Data with Pandas (Alternative)
```python
import pandas as pd

# Load partitioned synchronized data
df = pd.read_parquet("parsed_files/synced/", engine='pyarrow')

# Or load specific dates
df = pd.concat([
    pd.read_parquet("parsed_files/synced/2025-09-01.parquet"),
    pd.read_parquet("parsed_files/synced/2025-09-02.parquet")
], ignore_index=True)

# Basic analysis
print(f"Records: {len(df)}")
print(f"Temperature sensors: {len([col for col in df.columns if col.startswith('T_')])}")
print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
```

### Data Cleaning - Extract Casting Runs

After synchronization, you can clean the data to extract only "run" data (actual casting operations):

```bash
# Clean single parquet file
uv run python data_cleaning/cleaning.py --input parsed_files/synced/2025-09-02.parquet --output data_cleaning/cleaned_data/2025-09-02.parquet

# Clean entire directory
uv run python data_cleaning/cleaning.py --input parsed_files/synced/ --output data_cleaning/cleaned_data/
```

**What the cleaning script does:**
- Filters data to extract only "runs" (casting operations)
- A run starts when `sequence_id` appears (not 0 or null)
- A run ends when `end_of_cast == 1` for that `sequence_id`
- Keeps all rows from first `sequence_id` occurrence up to and including the first `end_of_cast == 1`
- Typically removes ~40% of non-run data (idle time, setup, etc.)

**Output:**
- Cleaned parquet files saved to `data_cleaning/cleaned_data/`
- Same structure as synced data, but filtered to only casting runs
- Ready for vectorization and machine learning analysis

### Jupyter Notebook Analysis
```bash
# Open the synced exploration notebook with Polars examples
jupyter notebook data_exploration/synced_exploration.ipynb
```

The notebook includes:
- Data loading and synchronization
- Polars integration for fast data processing
- Thermal profile visualization
- Process data analysis
- Statistical summaries
- Casting run filtering examples

## 📊 Data Collection Pattern

### Thermal Measurements
- **Port cycling**: 15→16→17→...→40→1→2→3→...→14→15→16→...
- **Time per port**: ~22 milliseconds per port measurement  
- **Full cycle time**: ~880ms to measure all 40 ports
- **Per-port sampling**: Each port measured approximately every 1 second

### Process Data
- **Cast length**: Continuous measurement during casting
- **End of cast**: Discrete events when casting stops
- **Sequences**: Process stage identification

## 🛠️ Setup

### Prerequisites
- Python 3.12+ (specified in `.python-version`)
- UV package manager

### Installation
```bash
# Clone or download the project
cd data_ingest

# Install dependencies using UV
uv sync

# Activate virtual environment (if needed)
# On Windows: .venv\Scripts\activate
# On Unix/Mac: source .venv/bin/activate
```

### Project Structure
- **`raw_data/`**: Place your input files here (*.WistDB, *.OPCB, etc.)
- **`parsed_files/`**: Generated output files (created automatically)
- **`data_exploration/`**: Jupyter notebooks for analysis
- **`read_binary/`**: Parser modules and utilities

## 🆘 Troubleshooting

**No data found?** Check that raw data files exist in `raw_data/`
**Synchronization issues?** Ensure all parsers have run successfully first
**Memory issues?** Process dates individually instead of all at once
**Missing columns?** Verify that all OPCB data types were parsed
**Notebook import errors?** Make sure you're running from the `data_ingest` directory or the notebook has the correct path setup

## 📈 Performance

- **WistDB parsing**: ~3-5 minutes per day
- **OPCB parsing**: ~1-2 minutes per day (10 parsers)
- **Synchronization**: ~1-2 minutes per day
- **Data cleaning**: ~30 seconds per day
- **Total processing**: ~6-10 minutes per day of data

## 🔬 Advanced Usage

### Data Cleaning Workflow

**Recommended workflow for analysis:**
1. **Sync Data**: Run `syncer.py` to synchronize and interpolate all data sources
2. **Data Cleaning**: Use `cleaning.py` to extract only casting runs (removes idle time)
3. **Analysis**: Use cleaned data for vectorization and ML models

```bash
# Step 1: Process all dates
uv run python main.py

# Step 2: Clean the synced data
uv run python data_ingest/data_cleaning/cleaning.py --input data_ingest/parsed_files/synced/ --output data_ingest/data_cleaning/cleaned_data/

# Step 3: Use cleaned data for analysis
import polars as pl
df = pl.read_parquet('data_ingest/data_cleaning/cleaned_data/2025-09-02.parquet')
```

### Custom Analysis
The synchronized data can be used for:
- Thermal profile analysis across the casting width
- Process optimization based on temperature patterns
- Quality control using end-of-cast events
- Sequence-based process monitoring
- Casting speed and width analysis
- Argon flow/pressure monitoring
- Temperature delta analysis (DeltaTLoose)
- Real-time process control applications

### Available Data Channels

The system extracts the following channels from OPCB files:

| Channel | Description | File Pattern |
|---------|-------------|--------------|
| `cast_length_m` | Steel length measurement | `*_castlength.csv` |
| `end_of_cast` | End-of-cast flag (0/1) | `*_endofcast.csv` |
| `sequence_id` | Process sequence identifier | `*_sequences.csv` |
| `slabs_grade` | Steel grade identification | `*_slabs_grade.csv` |
| `rec_casting_speed` | Recommended casting speed | `*_rec_casting_speed.csv` |
| `width_pv` | Width process variable | `*_width_pv.csv` |
| `deltatloose` | Temperature delta loose | `*_deltatloose.csv` |
| `ar_to_sen_flow` | Argon flow to SEN | `*_ar_to_sen_flow.csv` |
| `ar_to_sen_pressure` | Argon pressure to SEN | `*_ar_to_sen_pressure.csv` |
| `tundish_pos_rel` | Tundish position relative | `*_tundish_pos_rel.csv` |
| `casting_program` | Casting program identifier | `*_casting_program.csv` |
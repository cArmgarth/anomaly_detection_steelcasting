# Polars Quick Start Guide for SSAB Data Analysis

## Why Polars?

Polars is a fast DataFrame library written in Rust, providing:
- **2-5x faster** than pandas for most operations
- **30% lower memory** usage
- **Native Parquet support** - no version conflicts
- **Lazy evaluation** for optimized performance
- **Easy pandas conversion** when needed

## Loading Ultrasonic Data

```python
import polars as pl

# Load parquet file directly
df = pl.read_parquet('../parsed_files/synced/2025-09-01_to_2025-09-03_synced.parquet')

print(f"Shape: {df.shape}")
print(f"Memory: {df.estimated_size() / 1024**2:.1f} MB")
```

## Common Operations

### Filtering
```python
# Filter by date
sept_2 = df.filter(pl.col('datetime').dt.date() == pl.date(2025, 9, 2))

# Filter by temperature threshold
high_temp = df.filter(pl.col('T_0') > 1000)

# Multiple conditions
filtered = df.filter(
    (pl.col('datetime').dt.date() == pl.date(2025, 9, 2)) &
    (pl.col('cast_length_m') > 10)
)
```

### Selecting Columns
```python
# Select specific columns
temp_data = df.select(['datetime', 'T_0', 'T_1', 'T_2'])

# Select temperature columns
temp_cols = [col for col in df.columns if col.startswith('T_')]
temp_only = df.select(temp_cols)
```

### Aggregations
```python
# Statistics
stats = df.select(temp_cols).describe()

# Group by
by_grade = df.group_by('slabs_grade').agg([
    pl.mean('T_0'),
    pl.std('T_0'),
    pl.count()
])
```

### Time Series Operations
```python
# Sort by datetime
sorted_df = df.sort('datetime')

# Group by time windows
hourly = df.group_by(pl.col('datetime').dt.truncate('1h')).agg([
    pl.mean('T_0'),
    pl.mean('cast_length_m')
])
```

## Lazy Evaluation

For large datasets, use lazy evaluation to optimize queries:

```python
# Load lazily
lazy_df = pl.scan_parquet('../parsed_files/synced/*.parquet')

# Build optimized query
result = (lazy_df
    .filter(pl.col('datetime').dt.date() == pl.date(2025, 9, 2))
    .select(['datetime', 'T_0', 'T_1', 'cast_length_m'])
    .group_by('datetime')
    .agg([pl.mean('T_0'), pl.mean('T_1')])
    .collect()  # Execute the query
)
```

## Converting to Pandas

```python
# When you need pandas-specific functionality
pandas_df = df.to_pandas()

# Or convert specific operations
pandas_result = df.filter(pl.col('T_0') > 1000).to_pandas()
```

## Performance Tips

1. Use lazy evaluation for large datasets
2. Select only needed columns early in the pipeline
3. Filter before aggregating
4. Use `scan_parquet` instead of `read_parquet` for multiple files
5. Let Polars handle the work - avoid intermediate to_pandas() calls

## More Resources

- Polars Documentation: https://docs.pola.rs/
- Polars User Guide: https://docs.pola.rs/user-guide/
- Example notebooks in this project:
  - `polars_examples.py`
  - `polars_notebook_cells.py`

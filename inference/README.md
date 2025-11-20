# Inference Pipeline

Complete pipeline for running anomaly detection inference from raw data to predictions.

## Overview

The inference pipeline supports starting from different stages of the data processing pipeline:

1. **Raw Data** → Parse → Clean → Vectorize → Predict
2. **Parsed Data** → Clean → Vectorize → Predict
3. **Cleaned Data** → Vectorize → Predict
4. **Vectorized Data** → Predict

## Usage

### Command Line

#### From Raw Data Files

```bash
python inference/predict.py \
    --raw-wistdb raw_data/2025-09-02.WistDB \
    --raw-opcb raw_data/2025-09-02.OPCB \
    --model-dir model_training/models \
    --output-dir inference/results
```

#### From Parsed/Synced Data

```bash
python inference/predict.py \
    --parsed-data data_ingest/parsed_files/synced/2025-09-02.parquet \
    --model-dir model_training/models \
    --output-dir inference/results
```

#### From Cleaned Data

```bash
python inference/predict.py \
    --cleaned-data data_ingest/data_cleaning/cleaned_data/2025-09-02.parquet \
    --model-dir model_training/models \
    --output-dir inference/results
```

#### From Vectorized Data

```bash
python inference/predict.py \
    --vectorized-data feature_engineering/vectorized/vectorized.parquet \
    --model-dir model_training/models \
    --output-dir inference/results
```

### Options

- `--model-dir`: Directory containing model artifacts (default: `model_training/models`)
- `--output-dir`: Directory to save predictions (default: `inference/results`)
- `--no-attribution`: Skip feature attribution (faster but less detailed)
- `--top-n-features`: Number of top features to include in attribution (default: 5)
- `--verbose`: Enable verbose logging

### Output

The pipeline generates:

1. **Predictions Parquet**: `predictions_YYYYMMDD_HHMMSS.parquet`
   - Contains all cycles with predictions, anomaly scores, and metadata
   - Includes feature attribution (top N features and their deviations)

2. **Predictions CSV**: `predictions_YYYYMMDD_HHMMSS.csv`
   - Same data as parquet, in CSV format for easier inspection

### Output Columns

- `sequence_id`: Sequence identifier
- `cycle_id`: Cycle identifier within sequence
- `cycle_datetime`: Timestamp of the cycle
- `cast_length_m`: Cast length at cycle time
- `anomaly_score`: Anomaly score (lower = more anomalous)
- `is_anomaly`: Boolean indicating if cycle is an anomaly
- `prediction`: Raw prediction (-1 = anomaly, 1 = normal)
- `top_feature_1` through `top_feature_N`: Top contributing features
- `top_feature_1_deviation` through `top_feature_N_deviation`: Deviation scores for top features

## Python API

You can also use the inference pipeline programmatically:

```python
from pathlib import Path
from inference.predict import run_inference, load_model_artifacts

# Load model
model_dir = Path("model_training/models")
model, scaler, feature_cols, encoders, metadata = load_model_artifacts(model_dir)

# Run inference on vectorized data
vectorized_file = Path("feature_engineering/vectorized/vectorized.parquet")
results = run_inference(
    vectorized_file,
    model_dir,
    Path("inference/results"),
    include_attribution=True,
    top_n_features=5
)

# Access results
anomalies = results.filter(pl.col("is_anomaly"))
print(f"Found {len(anomalies)} anomalies")
```

## Pipeline Stages

### 1. Parsing (Raw Data → Parsed)

- Parses `.WistDB` and `.OPCB` binary files
- Extracts temperature, process variables, and metadata
- Synchronizes all data sources by timestamp
- Output: `data_ingest/parsed_files/synced/YYYY-MM-DD.parquet`

### 2. Cleaning (Parsed → Cleaned)

- Filters to casting runs only (sequence_id start to end_of_cast)
- Applies quality filters:
  - Casting program 5 or 7
  - Width PV == 1700
  - Complete sequences (cast_length_m start < 2)
  - Removes first 20m and last 10m of casting
- Output: `data_ingest/data_cleaning/cleaned_data/YYYY-MM-DD.parquet`

### 3. Vectorization (Cleaned → Vectorized)

- Identifies cycles (ports 17-32 measurement cycles)
- Extracts per-cycle features:
  - Temperature statistics (mean, std, min, max) for each sensor
  - Process variable statistics
  - Temporal features (cast length, casting program, etc.)
  - Rolling window features (means, deltas, trends)
- Output: `feature_engineering/vectorized/vectorized_YYYY-MM-DD.parquet`

### 4. Feature Preparation

- Encodes categorical features (casting_program, slabs_grade)
- Normalizes features per sequence
- Aligns with model's expected feature columns

### 5. Prediction

- Scales features using trained scaler
- Runs Isolation Forest model
- Calculates anomaly scores
- Optionally calculates feature attribution

## Model Requirements

The model directory must contain:

- `isolation_forest.joblib`: Trained Isolation Forest model (required)
- `scaler.joblib`: Fitted StandardScaler (required)
- `feature_columns.json`: List of feature column names (required)
- `label_encoders.joblib`: Label encoders for categorical features (optional - will be created if missing)
- `model_metadata.json`: Model metadata (optional)

## Notes

- **Missing encoders**: If `label_encoders.joblib` is not found, the pipeline will create new encoders during feature preparation. This works fine as long as the categorical values in your inference data match those seen during training. If you encounter unseen categorical values, they will be encoded as -1.
- Feature attribution requires additional computation but provides valuable insights
- All intermediate files are saved to `inference/temp/` for debugging
- The pipeline preserves all metadata (sequence_id, cycle_id, timestamps) for traceability


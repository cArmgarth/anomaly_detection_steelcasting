# Feature Documentation

This document explains all features used in the anomaly detection model.

## Overview

The model uses **1,133 features** extracted from casting process data. Features are computed per **cycle**, where one cycle = one complete measurement cycle through ports 17-32.

## Feature Categories

### 1. Temperature Sensor Features (284 features)

For each of the 71 temperature sensors (T_0 to T_70), we compute statistics across all ports 17-32 in each cycle:

#### Per-Sensor Statistics (4 features per sensor × 71 sensors = 284 features)
- **`T_{N}_mean`**: Mean temperature value for sensor N across ports 17-32 in the cycle
- **`T_{N}_std`**: Standard deviation of temperature for sensor N across ports 17-32
- **`T_{N}_min`**: Minimum temperature value for sensor N across ports 17-32
- **`T_{N}_max`**: Maximum temperature value for sensor N across ports 17-32
- **`T_{N}_range`**: Temperature range (max - min) for sensor N across ports 17-32

**Example**: `T_37_mean`, `T_37_std`, `T_37_min`, `T_37_max`, `T_37_range`

**Purpose**: Captures spatial temperature distribution for each sensor across the casting width (ports 17-32).

---

### 2. Overall Temperature Features (7 features)

Aggregated statistics across all sensors and ports:

- **`temp_overall_mean`**: Mean of all sensor means in the cycle
- **`temp_overall_std`**: Standard deviation of all sensor means
- **`temp_overall_min`**: Minimum temperature across all sensors and ports
- **`temp_overall_max`**: Maximum temperature across all sensors and ports
- **`temp_spatial_range`**: Overall temperature range (max - min)
- **`hot_spot_severity`**: Normalized hot spot indicator (how many standard deviations above mean is the max temp)
- **`cold_spot_severity`**: Normalized cold spot indicator (how many standard deviations below mean is the min temp)

**Purpose**: Captures overall temperature patterns and extreme conditions.

---

### 3. Spatial Gradient Features (2 features)

- **`avg_spatial_gradient`**: Average temperature range across all sensors (spatial variability)
- **`spatial_gradient_std`**: Standard deviation of spatial gradients across sensors

**Purpose**: Detects spatial temperature variations and gradients across the casting width.

---

### 4. Process Variable Features (12 features)

Process variables measured during each cycle:

#### Casting Speed
- **`rec_casting_speed_mean`**: Mean casting speed during the cycle
- **`rec_casting_speed_std`**: Standard deviation of casting speed

#### Delta T Loose
- **`deltatloose_mean`**: Mean delta T loose (temperature difference) during the cycle
- **`deltatloose_std`**: Standard deviation of delta T loose

#### Argon to SEN Flow
- **`ar_to_sen_flow_mean`**: Mean argon flow to SEN during the cycle
- **`ar_to_sen_flow_std`**: Standard deviation of argon flow

#### Argon to SEN Pressure
- **`ar_to_sen_pressure_mean`**: Mean argon pressure to SEN during the cycle
- **`ar_to_sen_pressure_std`**: Standard deviation of argon pressure

#### Tundish Position
- **`tundish_pos_rel_mean`**: Mean relative tundish position during the cycle
- **`tundish_pos_rel_std`**: Standard deviation of tundish position

#### Mould Level
- **`mould_level_mean`**: Mean mould level during the cycle
- **`mould_level_std`**: Standard deviation of mould level

**Purpose**: Captures process control variables that affect casting quality.

---

### 5. Temporal/Contextual Features (4 features)

- **`casting_program`**: Casting program identifier (categorical)
- **`slabs_grade`**: Steel grade identifier (categorical)
- **`cast_length_m`**: Cast length at cycle start (for anomaly localization)
- **`cycle_datetime`**: Timestamp of cycle start

**Purpose**: Provides context for the casting run and enables anomaly localization.

---

### 6. Rolling Window Features (~756 features)

Temporal features computed over a rolling window of the last 3 cycles. These capture trends and patterns over time.

#### Rolling Statistics (for key features only)
For each key feature (temperature sensor means, process variables, overall temp features), we compute:
- **`{feature}_rolling_mean_3`**: Rolling mean over last 3 cycles
- **`{feature}_rolling_std_3`**: Rolling standard deviation over last 3 cycles
- **`{feature}_rolling_min_3`**: Rolling minimum over last 3 cycles
- **`{feature}_rolling_max_3`**: Rolling maximum over last 3 cycles

#### Delta Features
- **`{feature}_delta`**: Change from previous cycle (current - previous)

#### Trend Indicators
- **`{feature}_trend`**: Trend indicator (1 if increasing, -1 if decreasing, 0 if stable)

#### Acceleration Features
- **`{feature}_acceleration`**: Rate of change of delta (second derivative)

**Key Features Used for Rolling Windows:**
- All temperature sensor means (T_0_mean through T_70_mean)
- Overall temperature features (temp_overall_*)
- Spatial gradient features
- Process variable means (rec_casting_speed_mean, deltatloose_mean, etc.)

**Purpose**: Captures temporal patterns, trends, and changes over time to detect gradual anomalies or process drifts.

---

## Feature Naming Conventions

### Temperature Sensors
- Format: `T_{N}_{statistic}`
- N: Sensor number (0-70)
- Statistics: `mean`, `std`, `min`, `max`, `range`

### Process Variables
- Format: `{variable}_{statistic}`
- Variables: `rec_casting_speed`, `deltatloose`, `ar_to_sen_flow`, `ar_to_sen_pressure`, `tundish_pos_rel`, `mould_level`
- Statistics: `mean`, `std`

### Rolling Window Features
- Format: `{base_feature}_{type}_{window_size}`
- Types: `rolling_mean`, `rolling_std`, `rolling_min`, `rolling_max`, `delta`, `trend`, `acceleration`
- Window size: `3` (for current implementation)

### Overall Features
- Prefix: `temp_overall_`, `avg_spatial_`, `spatial_`, `hot_spot_`, `cold_spot_`

---

## Feature Selection for Model

The model uses **1,133 features** total:
- **284** temperature sensor features (mean, std, min, max, range for 71 sensors)
- **7** overall temperature features
- **2** spatial gradient features
- **12** process variable features
- **4** temporal/contextual features
- **~756** rolling window features
- **6** metadata columns (sequence_id, cycle_id, cycle_datetime, casting_program, slabs_grade, cast_length_m) - excluded from model training

---

## Feature Importance

When analyzing anomalies, the model identifies **top contributing features** (top 5 by default). These indicate which sensors or process variables are most responsible for the anomaly:

- **Temperature features** (especially `_max`, `_min`, `_range`) often indicate hot/cold spots or spatial variations
- **Process variable features** indicate control issues or process deviations
- **Rolling window features** (especially `_delta`, `_trend`) indicate gradual changes or drifts
- **Overall features** (hot_spot_severity, cold_spot_severity) indicate extreme conditions

---

## Notes

- All features are computed per **cycle** (one complete pass through ports 17-32)
- Temperature features aggregate measurements across ports 17-32 for each sensor
- Rolling window features use a window size of 3 cycles
- Features are standardized (mean=0, std=1) before model training
- Missing values are handled during feature extraction and imputed if necessary


# Upstream Data Engineering Request: Fan Chart Data Population

## Context
The Properlytic UI requires fan chart data to display prediction intervals for property value forecasts. The database schema is ready but the columns are unpopulated.

## Target Table
**`h3_precomputed_hex_details`** (Supabase)

## Columns to Populate

### Fan Chart Columns
| Column | Description | Expected Value Type |
|--------|-------------|---------------------|
| `fan_p10_y1` | 10th percentile prediction, year 1 | Dollar value ($) |
| `fan_p10_y2` | 10th percentile prediction, year 2 | Dollar value ($) |
| `fan_p10_y3` | 10th percentile prediction, year 3 | Dollar value ($) |
| `fan_p10_y4` | 10th percentile prediction, year 4 | Dollar value ($) |
| `fan_p10_y5` | 10th percentile prediction, year 5 | Dollar value ($) |
| `fan_p50_y1` | 50th percentile (median), year 1 | Dollar value ($) |
| `fan_p50_y2` | 50th percentile (median), year 2 | Dollar value ($) |
| `fan_p50_y3` | 50th percentile (median), year 3 | Dollar value ($) |
| `fan_p50_y4` | 50th percentile (median), year 4 | Dollar value ($) |
| `fan_p50_y5` | 50th percentile (median), year 5 | Dollar value ($) |
| `fan_p90_y1` | 90th percentile prediction, year 1 | Dollar value ($) |
| `fan_p90_y2` | 90th percentile prediction, year 2 | Dollar value ($) |
| `fan_p90_y3` | 90th percentile prediction, year 3 | Dollar value ($) |
| `fan_p90_y4` | 90th percentile prediction, year 4 | Dollar value ($) |
| `fan_p90_y5` | 90th percentile prediction, year 5 | Dollar value ($) |

### Other Missing Data (UI shows N/A)

| Column | UI Element | Description | Current Status |
|--------|------------|-------------|----------------|
| `current_value` | Current Value | Current market/assessed value | NULL |
| `pred_cv` | Value Spread | Prediction coefficient of variation | NULL |
| `accuracy_term` | Confidence Factor | Model accuracy component | NULL |
| `confidence_term` | Confidence Factor | Prediction confidence component | NULL |
| `stability_term` | Confidence Factor | Time stability component | NULL |
| `robustness_term` | Confidence Factor | Model robustness component | NULL |
| `support_term` | Confidence Factor | Sample support component | NULL |
| `med_years` | Data History | Median years of data | NULL |

**Note**: Historical values (Data History sparkline) require querying `predicted_value` across multiple `forecast_year` values (2019-2025). This works but may be slow.

## Current State
- All 15 columns exist in the table
- All values are currently **NULL**
- Sample rows show `predicted_value` is populated (e.g., $4,120,885, $1,897,533)

## Required Output
For each H3 cell + forecast_year combination, compute the prediction interval fan:
- **p10**: 10th percentile (pessimistic scenario)
- **p50**: 50th percentile (median/expected value)
- **p90**: 90th percentile (optimistic scenario)

For years 1-5 into the future from the base forecast_year.

## Expected Source
This should come from either:
1. **Quantile regression** outputs from the ML model
2. **Monte Carlo simulation** of future values
3. **Bootstrap uncertainty** from training data

## Example Expected Values
For a hex with `predicted_value = $2,000,000`:
```
fan_p10_y1: 1,900,000   fan_p50_y1: 2,000,000   fan_p90_y1: 2,100,000
fan_p10_y2: 1,850,000   fan_p50_y2: 2,050,000   fan_p90_y2: 2,250,000
fan_p10_y3: 1,800,000   fan_p50_y3: 2,100,000   fan_p90_y3: 2,400,000
...
```

## UI Impact
Once populated, the fan chart will render showing prediction uncertainty over time, which is critical for user decision-making.

---

## ⚠️ DATA CONSISTENCY CONCERN

The UI currently merges data from **two tables** with field-level fallback:

| Table | Purpose | Has Data? |
|-------|---------|-----------|
| `h3_precomputed_hex_details` | Detailed metrics (49 cols) | Partial - many NULLs |
| `h3_precomputed_hex_rows` | Summary metrics (12 cols) | Yes - populated |

### Current Issue
We do **field-level fallback**: prefer `hex_details` value, fallback to `hex_rows` if NULL.

This means if the same field (e.g., `reliability`, `opportunity`) has **different values** in both tables, we could show inconsistent data depending on which table gets used.

### Example Discrepancy Found
| Field | hex_details (2025) | hex_rows (2026) |
|-------|-------------------|-----------------|
| `reliability` | 1.0 | 0.55-0.91 |
| `opportunity` | 0.0 | -0.007 to +0.94 |

### Request to Upstream
1. **Single source of truth**: Decide which table is canonical for each field
2. **Deprecate hex_rows**: If hex_details is the future schema, fully populate it and deprecate hex_rows
3. **Ensure consistency**: If both tables must coexist, ensure overlapping fields have identical values
4. **Document expected values**: Define expected ranges/semantics for each field

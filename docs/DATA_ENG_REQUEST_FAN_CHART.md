# Upstream Data Engineering Request: Fan Chart Data Population

## Context
The Properlytic UI requires fan chart data to display prediction intervals for property value forecasts. The database schema is ready but the columns are unpopulated.

## Target Table
**`h3_precomputed_hex_details`** (Supabase)

## Columns to Populate

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

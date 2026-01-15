# Upstream Data Engineering Request

> **Date**: 2026-01-14
> **From**: Properlytic UI Team
> **Priority**: HIGH - Multiple UI features blocked on data

---

## Executive Summary

The UI has schema in place for many features, but **data is not populated**. This document lists all missing data needed to enable core UI functionality.

---

## 🔴 Critical Missing Data

### 1. Fan Chart (Prediction Intervals)

**Table**: `h3_precomputed_hex_details`

| Column | Description | Status |
|--------|-------------|--------|
| `fan_p10_y1` - `fan_p10_y5` | 10th percentile, years 1-5 | ALL NULL |
| `fan_p50_y1` - `fan_p50_y5` | 50th percentile (median), years 1-5 | ALL NULL |
| `fan_p90_y1` - `fan_p90_y5` | 90th percentile, years 1-5 | ALL NULL |

**Expected source**: Quantile regression or Monte Carlo simulation

### Detailed Requirements

**Required Output**:
For each H3 cell + forecast_year combination, compute the prediction interval fan for years 1-5 into the future:
- **p10**: 10th percentile (pessimistic scenario)
- **p50**: 50th percentile (median/expected value)
- **p90**: 90th percentile (optimistic scenario)

**Example Expected Values**:
For a hex with `predicted_value = $2,000,000`:
```
fan_p10_y1: 1,900,000   fan_p50_y1: 2,000,000   fan_p90_y1: 2,100,000
fan_p10_y2: 1,850,000   fan_p50_y2: 2,050,000   fan_p90_y2: 2,250,000
...
```

**UI Impact**: Critical for rendering prediction uncertainty over time.

---

### 2. Current/Actual Values (for CAGR calculation)

**Table**: `h3_precomputed_hex_details`

| Column | Description | Status |
|--------|-------------|--------|
| `current_value` | Current market/assessed value | NULL |

**Issue**: Historical CAGR should use actual values, not predicted values. We have `predicted_value` per year but no `actual_value` or `assessed_value`.

**Request**: Either:
1. Populate `current_value` with actual market value for each forecast_year
2. Create new column `actual_value` per year
3. Create separate `h3_actuals` table with historical actual values

---

### 3. Confidence Factors (Reliability Decomposition)

**Table**: `h3_precomputed_hex_details`

| Column | UI Label | Status |
|--------|----------|--------|
| `accuracy_term` | Precision | NULL |
| `confidence_term` | Sample Size | NULL |
| `stability_term` | Consistency | NULL |
| `robustness_term` | Outlier Resistance | NULL |
| `support_term` | Data Coverage | NULL |

---

### 4. Value Spread / Uncertainty

**Table**: `h3_precomputed_hex_details`

| Column | Description | Status |
|--------|-------------|--------|
| `pred_cv` | Prediction coefficient of variation | NULL |
| `ape` | Absolute Percentage Error | NULL |
| `medae_z` | Median Absolute Error z-score | NULL |

---

### 5. Data History Metrics

**Table**: `h3_precomputed_hex_details`

| Column | Description | Status |
|--------|-------------|--------|
| `med_years` | Median years of data availability | NULL |

---

## ⚠️ Data Consistency Issue

### Two Tables with Overlapping Fields

| Table | Purpose | Status |
|-------|---------|--------|
| `h3_precomputed_hex_details` | Full schema (49 cols) | Many NULLs |
| `h3_precomputed_hex_rows` | Summary (12 cols) | Populated |

**Problem**: Same fields exist in both tables with **different values**:

| Field | hex_details (2025) | hex_rows (2026) |
|-------|-------------------|-----------------|
| `reliability` | 1.0 | 0.55-0.91 |
| `opportunity` | 0.0 | -0.007 to +0.94 |

**Request**:
1. Decide which table is canonical
2. Either fully populate hex_details (and deprecate hex_rows) OR ensure identical values where fields overlap
3. Document expected ranges/semantics for each field

---

## 📊 Data Anomaly Detected

### Predicted Values Jump at 2026

Sample data shows suspicious jump:

| Year | Predicted Value |
|------|----------------|
| 2025 | $4,120,885 |
| 2026 | $20,000,000 ← **5x jump** |
| 2027+ | $20,000,000 |

This looks like placeholder/test data. Please verify and correct if needed.

---

## Summary Checklist

- [ ] Populate fan chart columns (15 columns)
- [ ] Populate `current_value` or add actual values
- [ ] Populate confidence factor columns (5 columns)
- [ ] Populate `pred_cv`, `ape`, `medae_z`
- [ ] Populate `med_years`
- [ ] Resolve hex_details vs hex_rows data consistency
- [ ] Fix 2026+ value anomaly

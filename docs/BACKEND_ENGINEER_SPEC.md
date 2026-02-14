# Backend Engineer Specification: Unified Map Data Contract

**Status**: READY FOR IMPLEMENTATION  
**Date**: 2026-01-15  
**Context**: Optimization of Homecastr Map Frontend (Next.js/Supabase)

## 1. Executive Summary
The frontend requires a unified, optimized schema to reduce payload size and query complexity. The current system relies on multiple redundant tables (`h3_precomputed_hex_details`, `h3_precomputed_hex_rows`) and expensive joins. We are moving to a single partitioned table `h3_unified_forecasts` with a SQL View `view_h3_frontend_api` acting as the API contract.

**Key Requirements**:
1.  **Create New Schema**: `h3_unified_forecasts` (Partitioned by year).
2.  **Create View**: `view_h3_frontend_api` to handle derived logic.
3.  **Backfill & Populate**: Fill missing data (Fan Charts, Historicals) using specific logic defined in Section 4.
4.  **Infrastructure**: deploy required RPCs defined in Section 5.

---

## 2. Authorized Schema DDL

### A. The Storage Table
Create this table to store the raw values. Note the use of `REAL` and `REAL[]`.

```sql
CREATE TABLE h3_unified_forecasts (
    -- Keys
    h3_id           TEXT NOT NULL,
    forecast_year   INTEGER NOT NULL CHECK (forecast_year BETWEEN 2020 AND 2050),
    h3_res          INTEGER NOT NULL DEFAULT 9,
    
    -- Core Map Metrics (Float4)
    opportunity     REAL, -- "Growth"
    reliability     REAL, -- "Confidence" 0.0-1.0
    property_count  INTEGER DEFAULT 0,
    
    -- Proforma Metrics
    predicted_value REAL,
    noi             REAL,
    monthly_rent    REAL,
    dscr            REAL,
    cap_rate        REAL,
    breakeven_occ   REAL,
    liquidity       REAL,
    
    -- Risk & Quality
    risk_score      REAL,
    score           REAL,
    hard_fail       BOOLEAN DEFAULT FALSE,
    alert_pct       REAL,
    med_years       REAL,
    
    -- Advanced Risk Components
    tail_gap_z      REAL,
    medae_z         REAL,
    inv_dscr_z      REAL,
    
    -- Reliability Decomposition
    accuracy_term   REAL,
    confidence_term REAL,
    stability_term  REAL,
    robustness_term REAL,
    support_term    REAL,
    
    -- Fan Chart Arrays (Compressed Time-Series)
    -- Index 0=Year+1 ... Index 4=Year+5
    fan_p10         REAL[], 
    fan_p50         REAL[],
    fan_p90         REAL[],
    
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (forecast_year, h3_res, h3_id)
) PARTITION BY LIST (forecast_year);
```

### B. The API View
This view MUST be created to serve the frontend. It joins with the static grid table and computes derived fields.

```sql
CREATE OR REPLACE VIEW view_h3_frontend_api AS
SELECT
    f.h3_id,
    f.forecast_year,
    f.h3_res,
    
    -- Join Coordinates
    g.lat,
    g.lng,
    
    -- Metrics
    f.opportunity,
    f.opportunity * 100 as opportunity_pct, -- Computed
    f.reliability,
    f.property_count,
    f.med_years,
    f.alert_pct,
    
    -- Computed Trend Logic
    CASE 
        WHEN f.opportunity > 0.05 THEN 'up'
        WHEN f.opportunity < -0.02 THEN 'down'
        ELSE 'stable'
    END as trend,
    
    -- Proforma
    f.predicted_value, f.noi, f.monthly_rent, f.dscr, f.cap_rate, f.breakeven_occ, f.liquidity,
    
    -- Risk
    f.risk_score, f.score, f.hard_fail, f.tail_gap_z, f.medae_z, f.inv_dscr_z,
    
    -- Composition
    f.accuracy_term, f.confidence_term, f.stability_term, f.robustness_term, f.support_term,
    
    -- Unroll Arrays for Legacy Frontend Compatibility
    f.fan_p10[1] as fan_p10_y1, f.fan_p10[2] as fan_p10_y2, f.fan_p10[3] as fan_p10_y3, 
    f.fan_p10[4] as fan_p10_y4, f.fan_p10[5] as fan_p10_y5,
    
    f.fan_p50[1] as fan_p50_y1, f.fan_p50[2] as fan_p50_y2, f.fan_p50[3] as fan_p50_y3, 
    f.fan_p50[4] as fan_p50_y4, f.fan_p50[5] as fan_p50_y5,
    
    f.fan_p90[1] as fan_p90_y1, f.fan_p90[2] as fan_p90_y2, f.fan_p90[3] as fan_p90_y3, 
    f.fan_p90[4] as fan_p90_y4, f.fan_p90[5] as fan_p90_y5

FROM h3_unified_forecasts f
JOIN h3_aoi_grid g ON f.h3_id = g.h3_id AND f.h3_res = g.h3_res;
```

---

## 3. Migration & Backfill Strategy

### Step 1: Run DDL
Execute the SQL above to create the table partitions (you will need to create partitions for 2020-2030) and the view.

### Step 2: Backfill Data
Use the following pattern to populate the new table from the old `h3_precomputed_hex_details`. Note that you will need to compute/extract the array values as detailed in Section 4.

```sql
INSERT INTO h3_unified_forecasts (
    h3_id, forecast_year, h3_res,
    opportunity, reliability, property_count,
    predicted_value, noi, monthly_rent, dscr, cap_rate, breakeven_occ, liquidity,
    risk_score, score, hard_fail, alert_pct, med_years,
    tail_gap_z, medae_z, inv_dscr_z,
    accuracy_term, confidence_term, stability_term, robustness_term, support_term,
    fan_p10, fan_p50, fan_p90
)
SELECT 
    h3_id, forecast_year, h3_res,
    opportunity, reliability, property_count,
    predicted_value, noi, monthly_rent, dscr, cap_rate, breakeven_occ, liquidity,
    risk_score, score, (hard_fail = 't'), alert_pct, med_years,
    tail_gap_z, medae_z, inv_dscr_z,
    accuracy_term, confidence_term, stability_term, robustness_term, support_term,
    -- Construct Arrays from legacy columns if available, otherwise NULL
    ARRAY[fan_p10_y1, fan_p10_y2, fan_p10_y3, fan_p10_y4, fan_p10_y5],
    ARRAY[fan_p50_y1, fan_p50_y2, fan_p50_y3, fan_p50_y4, fan_p50_y5],
    ARRAY[fan_p90_y1, fan_p90_y2, fan_p90_y3, fan_p90_y4, fan_p90_y5]
FROM h3_precomputed_hex_details
WHERE forecast_year BETWEEN 2026 AND 2030; -- Process in batches
```

---

## 4. Pipeline & Data Population Requirements

The following data is currently MISSING or BROKEN in the source tables and must be fixed during the population of `h3_unified_forecasts`.

### A. Fan Chart (Prediction Intervals)
**Requirement**: Populate `fan_p10`, `fan_p50`, `fan_p90` arrays.
*   **Source**: Quantile regression or Monte Carlo simulation from the ML pipeline.
*   **Logic**:
    *   `fan_p10`: 10th percentile (pessimistic)
    *   `fan_p50`: Median (expected)
    *   `fan_p90`: 90th percentile (optimistic)
*   **Current Issue**: Pipeline explicitly fills these with `NaN`. This must be implemented.

### B. Historical Data (2019-2025)
**Requirement**: Populate `h3_unified_forecasts` for historical years (2020, 2021, etc.).
*   **Predicted Value**: For history, `predicted_value` should store the **Actual/Assessed Value** (median of the hex).
*   **Current Issue**: Historical rows are missing entirely or have NULL `med_predicted_value`, causing the map to be blank in historical mode.
*   **Action**: Ensure historical actuals are mapped to `predicted_value` in this schema.

### C. Reliability Components
**Requirement**: Populate decomposition terms.
*   `accuracy_term`, `confidence_term`, `stability_term`, `robustness_term`, `support_term`.
*   These are currently NULL in source tables but required for the "Confidence Breakdown" UI.

---

## 5. Infrastructure Requirements (RPCs)

### A. Parcel Geometry RPC
**Blocker**: Frontend cannot fetch lot lines for Max Zoom level.
**Action**: Create the following RPC function in Supabase.

```sql
create or replace function get_parcels_in_bounds(min_lat float, min_lng float, max_lat float, max_lng float)
returns table (
  acct_key text,
  geom geometry
)
language plpgsql
as $$
begin
  return query
  select p.acct_key, p.geom
  from parcels p
  -- Spatial index usage is critical here
  where p.geom && ST_MakeEnvelope(min_lng, min_lat, max_lng, max_lat, 4326)
  limit 2000;
end;
$$;
```

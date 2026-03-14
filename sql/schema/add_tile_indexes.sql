-- =================================================================
-- Phase 1: Add covering compound indexes for MVT tile performance
-- 
-- The _mvt_forecast_generic query filters on:
--   (origin_year, horizon_m, series_kind, variant_id, <geoid>)
-- and selects (p50, value) for growth_pct computation.
--
-- These covering indexes match the exact access pattern.
-- CONCURRENTLY avoids locking the table during creation.
-- =================================================================

-- Tract metrics (z8-z11 tiles, ~84K geoids)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fq_tract_tile
  ON forecast_queue.metrics_tract_forecast(origin_year, horizon_m, series_kind, variant_id, tract_geoid20)
  INCLUDE (p50, value);

-- ZCTA metrics (z5-z7 tiles, ~33K ZCTAs)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fq_zcta_tile
  ON forecast_queue.metrics_zcta_forecast(origin_year, horizon_m, series_kind, variant_id, zcta5)
  INCLUDE (p50, value);

-- Tabblock metrics (z12-z16 tiles, ~8M blocks)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fq_tabblock_tile
  ON forecast_queue.metrics_tabblock_forecast(origin_year, horizon_m, series_kind, variant_id, tabblock_geoid20)
  INCLUDE (p50, value);

-- State metrics (z0-z4 tiles, ~52 states)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fq_state_tile
  ON forecast_queue.metrics_state_forecast(origin_year, horizon_m, series_kind, variant_id, state_fips)
  INCLUDE (p50, value);

-- ANALYZE to update planner statistics
ANALYZE forecast_queue.metrics_tract_forecast;
ANALYZE forecast_queue.metrics_zcta_forecast;
ANALYZE forecast_queue.metrics_tabblock_forecast;
ANALYZE forecast_queue.metrics_state_forecast;

-- Verify indexes were created
SELECT indexname, tablename, indexdef 
FROM pg_indexes 
WHERE schemaname = 'forecast_queue' 
  AND indexname LIKE 'idx_fq_%_tile'
ORDER BY tablename;

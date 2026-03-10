-- =====================================================================
-- Migration: Rename geo_tract20_tx → geo_tract20_us
-- The tract geometry table is national (85k+ tracts, all 53 states)
-- but has a legacy _tx suffix from initial Texas onboarding.
-- =====================================================================

-- 1) Rename tables
ALTER TABLE IF EXISTS public.geo_tract20_tx RENAME TO geo_tract20_us;
ALTER TABLE IF EXISTS public.geo_tract20_tx_z10 RENAME TO geo_tract20_us_z10;

-- 2) Rename indexes (PostgreSQL renames the constraint automatically for PKs,
--    but explicitly rename GIST indexes for clarity)
ALTER INDEX IF EXISTS geo_tract20_tx_pkey RENAME TO geo_tract20_us_pkey;
ALTER INDEX IF EXISTS idx_geo_tract20_tx_geom RENAME TO idx_geo_tract20_us_geom;
ALTER INDEX IF EXISTS geo_tract20_tx_geom_gix RENAME TO geo_tract20_us_geom_gix;
ALTER INDEX IF EXISTS geo_tract20_tx_z10_pkey RENAME TO geo_tract20_us_z10_pkey;
ALTER INDEX IF EXISTS geo_tract20_tx_z10_geom_gix RENAME TO geo_tract20_us_z10_geom_gix;

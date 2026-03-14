-- Persistent slug → tract_geoid lookup table
-- Eliminates redundant county-wide enrichment on every forecast page load
-- Run this in the Supabase SQL Editor

CREATE TABLE IF NOT EXISTS public.tract_slug_lookup (
    state_slug        TEXT NOT NULL,
    city_slug         TEXT NOT NULL,
    neighborhood_slug TEXT NOT NULL,
    tract_geoid       TEXT NOT NULL,
    schema_name       TEXT NOT NULL DEFAULT 'forecast_queue',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (state_slug, city_slug, neighborhood_slug, schema_name)
);

CREATE INDEX IF NOT EXISTS idx_slug_lookup_geoid
    ON public.tract_slug_lookup (tract_geoid);

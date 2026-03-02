-- Street View image cache
-- Run in Supabase Dashboard → SQL Editor
--
-- Each row maps a (lat, lng, size) key to a GCS object path.
-- The primary key prevents duplicate fetches from concurrent requests.

CREATE TABLE IF NOT EXISTS public.street_view_cache (
    lat5       TEXT NOT NULL,          -- latitude  rounded to 5 decimal places
    lng5       TEXT NOT NULL,          -- longitude rounded to 5 decimal places
    w          INT  NOT NULL,          -- image width  (px)
    h          INT  NOT NULL,          -- image height (px)
    gcs_path   TEXT NOT NULL,          -- e.g. "streetview/29.76100,-95.36200_400x300.jpg"
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (lat5, lng5, w, h)
);

-- Accessed only via service-role key from server-side API routes
ALTER TABLE public.street_view_cache ENABLE ROW LEVEL SECURITY;
-- No policies needed — service role bypasses RLS

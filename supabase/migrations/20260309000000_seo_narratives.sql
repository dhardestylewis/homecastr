-- Table to store AI-generated narratives for SEO pages
-- Run in Supabase Dashboard -> SQL Editor
-- Or deploy via migration tooling

CREATE TABLE IF NOT EXISTS public.seo_narratives (
    geoid          TEXT NOT NULL,
    level          TEXT NOT NULL, -- e.g., 'state', 'county', 'city', 'tract'
    narrative_json JSONB NOT NULL,
    model_version  TEXT NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (geoid, level)
);

-- Enable RLS to secure data modifications
ALTER TABLE public.seo_narratives ENABLE ROW LEVEL SECURITY;

-- Allow read access for everyone (for public SEO pages if requested from client)
-- But typically this is fetched server-side in Next.js Server Components
CREATE POLICY "Allow public read access to seo_narratives" 
ON public.seo_narratives 
FOR SELECT 
USING (true);

-- No insert/update policies needed as it's modified via service role from Modal

CREATE TABLE IF NOT EXISTS public.outlier_flags (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tract_geoid VARCHAR(11) NOT NULL,
    reason TEXT NOT NULL,
    h12 NUMERIC,
    h60 NUMERIC,
    schema_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    resolved BOOLEAN DEFAULT FALSE,
    UNIQUE(tract_geoid, schema_name)
);

-- Enable RLS (though frontend writes via service role)
ALTER TABLE public.outlier_flags ENABLE ROW LEVEL SECURITY;

-- Create an index to quickly find unresolved outliers
CREATE INDEX IF NOT EXISTS idx_outlier_flags_unresolved ON public.outlier_flags(resolved) WHERE resolved = FALSE;

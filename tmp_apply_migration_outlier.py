import psycopg2

DB_URL = "postgres://postgres.earrhbknfjnhbudsucch:Every1sentence!@aws-1-us-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
conn = psycopg2.connect(DB_URL, connect_timeout=30)
cur = conn.cursor()

sql = """
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

ALTER TABLE public.outlier_flags ENABLE ROW LEVEL SECURITY;
CREATE INDEX IF NOT EXISTS idx_outlier_flags_unresolved ON public.outlier_flags(resolved) WHERE resolved = FALSE;
"""

try:
    cur.execute(sql)
    conn.commit()
    print("Successfully created outlier_flags table.")
except Exception as e:
    print("Error:", e)
    conn.rollback()

conn.close()

-- Chat threads table for persisting assistant conversations
-- Enables shareable thread URLs (?thread=abc123) and session persistence

CREATE TABLE IF NOT EXISTS public.chat_threads (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    
    -- Context: what forecast/page this thread is associated with
    tract_geoid TEXT,
    neighborhood_name TEXT,
    city TEXT,
    state TEXT,
    current_url TEXT,
    
    -- Thread content
    messages JSONB NOT NULL DEFAULT '[]'::jsonb,
    
    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    
    -- Optional: user identifier for logged-in users
    user_id TEXT
);

-- Index for quick lookups
CREATE INDEX IF NOT EXISTS idx_chat_threads_updated_at ON public.chat_threads(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_threads_tract ON public.chat_threads(tract_geoid);

-- Enable RLS but allow public read/write for now (anonymous users can create threads)
ALTER TABLE public.chat_threads ENABLE ROW LEVEL SECURITY;

-- Policy: anyone can read threads (for shareable URLs)
CREATE POLICY "Allow public read access to chat_threads" 
    ON public.chat_threads 
    FOR SELECT 
    USING (true);

-- Policy: anyone can insert threads
CREATE POLICY "Allow public insert to chat_threads" 
    ON public.chat_threads 
    FOR INSERT 
    WITH CHECK (true);

-- Policy: anyone can update their thread (by knowing the ID)
CREATE POLICY "Allow public update to chat_threads" 
    ON public.chat_threads 
    FOR UPDATE 
    USING (true);

COMMENT ON TABLE public.chat_threads IS 'Persisted assistant chat threads for shareable URLs and session continuity';

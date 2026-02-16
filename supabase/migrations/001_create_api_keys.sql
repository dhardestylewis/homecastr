-- Create api_keys table for storing generated API keys
-- Run this in your Supabase SQL Editor (Dashboard > SQL Editor > New Query)

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    email TEXT NOT NULL,
    key TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT now(),
    revoked_at TIMESTAMPTZ DEFAULT NULL
);

-- Index for fast key lookups during API auth
CREATE INDEX IF NOT EXISTS idx_api_keys_key ON api_keys (key);

-- Index for looking up keys by email
CREATE INDEX IF NOT EXISTS idx_api_keys_email ON api_keys (email);

-- Disable RLS since this table is only accessed via service role from server
ALTER TABLE api_keys ENABLE ROW LEVEL SECURITY;

-- No RLS policies needed — service role bypasses RLS

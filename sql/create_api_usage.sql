-- API usage logging table
-- Run this in the Supabase SQL Editor

create table if not exists public.api_usage (
  id         bigint generated always as identity primary key,
  ts         timestamptz not null default now(),
  endpoint   text        not null,
  method     text        not null default 'GET',
  key_id     text,                          -- 'demo', 'rapidapi', or uuid
  status     int         not null,
  latency_ms int,
  ip         text,
  source     text        not null default 'direct'  -- 'direct' | 'rapidapi' | 'demo'
);

-- Index for time-range + endpoint queries
create index if not exists idx_api_usage_ts_endpoint
  on public.api_usage (ts desc, endpoint);

-- Index for per-key queries
create index if not exists idx_api_usage_key_id
  on public.api_usage (key_id, ts desc);

-- Allow service-role inserts (RLS disabled for this table since only
-- the server-side admin client writes to it)
alter table public.api_usage enable row level security;
-- No RLS policies → only service-role key can read/write

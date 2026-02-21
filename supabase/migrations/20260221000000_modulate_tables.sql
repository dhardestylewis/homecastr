-- Modulate.ai voice intelligence tables for conversation analysis

CREATE TABLE IF NOT EXISTS modulate_events (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  conversation_id text NOT NULL,
  event_type    text NOT NULL,
  properties    jsonb DEFAULT '{}',
  created_at    timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_modulate_events_conversation
  ON modulate_events (conversation_id);

CREATE TABLE IF NOT EXISTS modulate_analyses (
  id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  conversation_id     text NOT NULL UNIQUE,
  transcript          jsonb DEFAULT '[]',
  safety_score        real,
  compliance_score    real,
  emotion_timeline    jsonb DEFAULT '[]',
  safety_flags        jsonb DEFAULT '[]',
  deepfake_synthetic  boolean DEFAULT false,
  deepfake_confidence real DEFAULT 0,
  off_script_detected boolean DEFAULT false,
  off_script_instances jsonb DEFAULT '[]',
  summary             text,
  created_at          timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_modulate_analyses_conversation
  ON modulate_analyses (conversation_id);
CREATE INDEX IF NOT EXISTS idx_modulate_analyses_safety
  ON modulate_analyses (safety_score);

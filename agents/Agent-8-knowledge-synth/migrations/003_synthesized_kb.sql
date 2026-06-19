CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS sentinel_synthesized_kb (
  id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  cluster_signature       TEXT NOT NULL,
  version                 INT NOT NULL,
  is_active               BOOLEAN NOT NULL DEFAULT TRUE,
  title                   TEXT NOT NULL,
  problem_summary         TEXT NOT NULL,
  root_cause              TEXT,
  resolution_steps        JSONB NOT NULL,
  keywords                TEXT[],
  assignment_group        TEXT NOT NULL,
  category                TEXT,
  subcategory             TEXT,
  source_incident_ids     TEXT[] NOT NULL,
  source_incident_count   INT GENERATED ALWAYS AS (cardinality(source_incident_ids)) STORED,
  confidence_score        NUMERIC(4,3) NOT NULL,
  llm_self_rating         NUMERIC(4,3),
  cluster_cohesion        NUMERIC(4,3),
  embedding_title         VECTOR(768),
  embedding_full          VECTOR(768),
  confluence_page_id      TEXT,
  embedding_model_version TEXT NOT NULL,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  retired_at              TIMESTAMPTZ,
  UNIQUE (cluster_signature, version)
);

CREATE INDEX IF NOT EXISTS idx_skb_active    ON sentinel_synthesized_kb (is_active);
CREATE INDEX IF NOT EXISTS idx_skb_assigngrp ON sentinel_synthesized_kb (assignment_group) WHERE is_active;
CREATE INDEX IF NOT EXISTS idx_skb_title_vec ON sentinel_synthesized_kb USING ivfflat (embedding_title vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_skb_full_vec  ON sentinel_synthesized_kb USING ivfflat (embedding_full vector_cosine_ops);

CREATE TABLE IF NOT EXISTS kb_synthesis_runs (
  run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at     TIMESTAMPTZ,
  status          TEXT CHECK (status IN ('running','succeeded','partial','failed')),
  window_start    DATE NOT NULL,
  window_end      DATE NOT NULL,
  counts          JSONB NOT NULL DEFAULT '{}'::jsonb,
  stage_durations JSONB,
  error_message   TEXT
);

CREATE TABLE IF NOT EXISTS kb_synthesis_decisions (
  decision_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id            UUID NOT NULL REFERENCES kb_synthesis_runs(run_id),
  cluster_signature TEXT NOT NULL,
  decision          TEXT NOT NULL CHECK (decision IN ('create','update','review','skip')),
  article_id        UUID REFERENCES sentinel_synthesized_kb(id),
  similarity_score  NUMERIC(4,3),
  notes             TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_decisions_run ON kb_synthesis_decisions (run_id);

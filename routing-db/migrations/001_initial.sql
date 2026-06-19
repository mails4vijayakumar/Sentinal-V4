-- ═════════════════════════════════════════════════════════════════════════════
--  Sentinel — Migration 001: Initial Schema
--  Database: sentinel
--  Schemas: routing · feedback · chat · kb
-- ═════════════════════════════════════════════════════════════════════════════

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "vector";

-- ── Schemas ───────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS routing;
CREATE SCHEMA IF NOT EXISTS feedback;
CREATE SCHEMA IF NOT EXISTS chat;
CREATE SCHEMA IF NOT EXISTS kb;

-- ════════════════════════════════════════════════════════════════
--  ROUTING SCHEMA
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS routing.incidents (
    id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    external_id  TEXT        NOT NULL,
    source       TEXT        NOT NULL CHECK (source IN ('dynatrace','servicenow')),
    severity     TEXT        NOT NULL CHECK (severity IN ('P1','P2','P3','P4','P5')),
    flow         TEXT        NOT NULL CHECK (flow IN ('primary','secondary')),
    title        TEXT        NOT NULL,
    description  TEXT,
    host         TEXT,
    service      TEXT,
    raw_payload  JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_incidents_external_id UNIQUE (external_id)
);

CREATE TABLE IF NOT EXISTS routing.pipeline_runs (
    id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id  UUID        NOT NULL REFERENCES routing.incidents(id) ON DELETE CASCADE,
    status       TEXT        NOT NULL DEFAULT 'running'
                             CHECK (status IN ('running','completed','failed','cancelled')),
    flow         TEXT        NOT NULL CHECK (flow IN ('primary','secondary')),
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    duration_ms  INTEGER,
    meta         JSONB       DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS routing.pipeline_steps (
    id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id       UUID        NOT NULL REFERENCES routing.pipeline_runs(id) ON DELETE CASCADE,
    agent_num    SMALLINT    NOT NULL CHECK (agent_num BETWEEN 1 AND 7),
    agent_name   TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending','running','completed','failed','skipped')),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    duration_ms  INTEGER,
    summary      TEXT,
    error        TEXT,
    retry_count  SMALLINT    DEFAULT 0,
    CONSTRAINT uq_steps_run_agent UNIQUE (run_id, agent_num)
);

CREATE TABLE IF NOT EXISTS routing.enrichments (
    id         UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id     UUID        NOT NULL REFERENCES routing.pipeline_runs(id) ON DELETE CASCADE,
    agent_num  SMALLINT    NOT NULL,
    source     TEXT        NOT NULL,
    data       JSONB       NOT NULL DEFAULT '{}',
    written_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS routing.snow_config (
    config_key   TEXT PRIMARY KEY,
    config_value TEXT NOT NULL,
    description  TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS routing.snow_records (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id  UUID NOT NULL REFERENCES routing.incidents(id),
    snow_number  TEXT NOT NULL,
    snow_sys_id  TEXT NOT NULL,
    state        TEXT NOT NULL DEFAULT 'new',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS routing.pd_records (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_id    UUID NOT NULL REFERENCES routing.incidents(id),
    pd_incident_id TEXT NOT NULL,
    pd_incident_key TEXT,
    service_id     TEXT,
    status         TEXT NOT NULL DEFAULT 'triggered',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ════════════════════════════════════════════════════════════════
--  FEEDBACK SCHEMA
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS feedback.resolutions (
    id               UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id           UUID    NOT NULL,
    incident_id      UUID    NOT NULL,
    root_cause       TEXT,
    root_cause_cat   TEXT,
    resolution_steps JSONB   DEFAULT '[]',
    confidence       INTEGER CHECK (confidence BETWEEN 0 AND 100),
    llm_provider     TEXT,
    llm_model        TEXT,
    tokens_used      INTEGER,
    generated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS feedback.ratings (
    id            UUID     PRIMARY KEY DEFAULT uuid_generate_v4(),
    resolution_id UUID     NOT NULL REFERENCES feedback.resolutions(id),
    rated_by      TEXT,
    rating        SMALLINT CHECK (rating BETWEEN 1 AND 5),
    comment       TEXT,
    rated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ════════════════════════════════════════════════════════════════
--  CHAT SCHEMA
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS chat.sessions (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id      TEXT NOT NULL,
    title        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_active  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chat.messages (
    id           UUID     PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id   UUID     NOT NULL REFERENCES chat.sessions(id) ON DELETE CASCADE,
    role         TEXT     NOT NULL CHECK (role IN ('user','assistant','system')),
    content      TEXT     NOT NULL,
    sources      JSONB    DEFAULT '[]',    -- KB citations attached to this turn
    token_count  INTEGER,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ════════════════════════════════════════════════════════════════
--  KB SCHEMA (pgvector RAG)
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS kb.documents (
    id           UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_type  TEXT    NOT NULL,
    source_id    TEXT    NOT NULL,
    source_url   TEXT,
    title        TEXT    NOT NULL,
    content      TEXT    NOT NULL,
    content_hash TEXT    NOT NULL,
    metadata     JSONB   DEFAULT '{}',
    indexed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_type, source_id),
    UNIQUE (content_hash)
);

CREATE TABLE IF NOT EXISTS kb.chunks (
    id          UUID     PRIMARY KEY DEFAULT uuid_generate_v4(),
    document_id UUID     NOT NULL REFERENCES kb.documents(id) ON DELETE CASCADE,
    chunk_index SMALLINT NOT NULL,
    content     TEXT     NOT NULL,
    embedding   vector(768),
    token_count INTEGER,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (document_id, chunk_index)
);

-- HNSW index: m=16, ef_construction=64 — balanced for healthcare doc volumes
CREATE INDEX IF NOT EXISTS idx_kb_chunks_embedding
    ON kb.chunks USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- ── Standard indexes ──────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_incidents_external    ON routing.incidents(external_id);
CREATE INDEX IF NOT EXISTS idx_incidents_severity    ON routing.incidents(severity, flow);
CREATE INDEX IF NOT EXISTS idx_incidents_created     ON routing.incidents(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_incident         ON routing.pipeline_runs(incident_id);
CREATE INDEX IF NOT EXISTS idx_runs_status           ON routing.pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_started          ON routing.pipeline_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_steps_run             ON routing.pipeline_steps(run_id, agent_num);
CREATE INDEX IF NOT EXISTS idx_enrichments_run       ON routing.enrichments(run_id, agent_num);
CREATE INDEX IF NOT EXISTS idx_resolutions_run       ON feedback.resolutions(run_id);
CREATE INDEX IF NOT EXISTS idx_chat_messages_session ON chat.messages(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_kb_docs_source        ON kb.documents(source_type, source_id);

-- GIN indexes for JSONB queries
CREATE INDEX IF NOT EXISTS idx_enrichments_data_gin ON routing.enrichments USING gin(data);
CREATE INDEX IF NOT EXISTS idx_incidents_payload_gin ON routing.incidents USING gin(raw_payload);

-- ── Auto-update updated_at ────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION routing.touch_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$;

CREATE OR REPLACE TRIGGER trg_incidents_updated_at
    BEFORE UPDATE ON routing.incidents
    FOR EACH ROW EXECUTE FUNCTION routing.touch_updated_at();

CREATE OR REPLACE TRIGGER trg_snow_records_updated_at
    BEFORE UPDATE ON routing.snow_records
    FOR EACH ROW EXECUTE FUNCTION routing.touch_updated_at();

CREATE OR REPLACE TRIGGER trg_pd_records_updated_at
    BEFORE UPDATE ON routing.pd_records
    FOR EACH ROW EXECUTE FUNCTION routing.touch_updated_at();

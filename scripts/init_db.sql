-- PostgreSQL 16 + pgvector Initialization Script for Aether
-- Enables pgvector extension, telemetry schema, incident tracking, and remediation audit logs.

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Telemetry Logs Table (High-throughput log ingestion + semantic embeddings)
CREATE TABLE IF NOT EXISTS telemetry_logs (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) UNIQUE NOT NULL,               -- Deduplication key (UUID/ULID)
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service_name VARCHAR(100) NOT NULL,
    level VARCHAR(20) NOT NULL,                         -- INFO, WARN, ERROR, CRITICAL
    message TEXT NOT NULL,
    trace_id VARCHAR(64),
    span_id VARCHAR(32),
    http_status INT,
    duration_ms DOUBLE PRECISION,
    metadata JSONB DEFAULT '{}'::jsonb,
    embedding vector(384)                               -- MiniLM / FastEmbed vector size
);

-- Index for temporal and service filtering
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON telemetry_logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_service_level ON telemetry_logs (service_name, level);
CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON telemetry_logs (trace_id) WHERE trace_id IS NOT NULL;

-- HNSW Vector index for fast approximate nearest neighbor semantic search on error logs
CREATE INDEX IF NOT EXISTS idx_logs_embedding_hnsw 
ON telemetry_logs 
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- 2. Deployment History Table (Tracks commits, image tags, config diffs)
CREATE TABLE IF NOT EXISTS deployment_history (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(100) NOT NULL,
    version VARCHAR(50) NOT NULL,                       -- e.g. "v1.4.2"
    commit_sha VARCHAR(40) NOT NULL,
    config_diff JSONB DEFAULT '{}'::jsonb,              -- Exact key-value config changes
    deployed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    deployed_by VARCHAR(100) DEFAULT 'gitops-ci'
);

CREATE INDEX IF NOT EXISTS idx_deployments_service_active ON deployment_history (service_name, is_active);

-- Seed initial healthy baseline deployment
INSERT INTO deployment_history (service_name, version, commit_sha, config_diff, deployed_at, is_active, deployed_by)
VALUES (
    'payment-service',
    'v1.0.0',
    'a7f2910c438bde1940172834b9281a0293817420',
    '{"db_pool_size": 20, "db_timeout_ms": 2000, "retry_attempts": 3}'::jsonb,
    NOW() - INTERVAL '2 hours',
    TRUE,
    'github-actions'
) ON CONFLICT DO NOTHING;

-- 3. Incidents Table (Deterministic anomaly tracking & RCA lifecycle)
CREATE TABLE IF NOT EXISTS incidents (
    incident_id VARCHAR(64) PRIMARY KEY,                -- e.g. "INC-2026-0916-01"
    service_name VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,                      -- P1, P2, P3
    status VARCHAR(30) NOT NULL DEFAULT 'DETECTED',     -- DETECTED, TRIAGING, REMEDIATING, VERIFYING, RESOLVED, ROLLED_BACK, REQUIRE_HUMAN_TRIAGE
    trigger_reason TEXT NOT NULL,                       -- e.g. "SLO Breach: HTTP 500 error rate = 24.5% (> 5% limit)"
    metric_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    trace_evidence JSONB DEFAULT '{}'::jsonb,
    rca_summary TEXT,                                   -- LangGraph diagnostic summary
    confidence_score DOUBLE PRECISION,                  -- 0.0 to 1.0
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents (status);
CREATE INDEX IF NOT EXISTS idx_incidents_service ON incidents (service_name, created_at DESC);

-- 4. Remediation Audit Log Table (Zero-Trust Policy & Execution History with Idempotency)
CREATE TABLE IF NOT EXISTS remediation_audit_log (
    remediation_id VARCHAR(64) PRIMARY KEY,             -- e.g. "REM-2026-0916-01"
    incident_id VARCHAR(64) NOT NULL REFERENCES incidents(incident_id),
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,       -- Hash of (incident_id, action_type, target)
    action_type VARCHAR(50) NOT NULL,                   -- ROLLBACK_DEPLOYMENT, SCALE_REPLICAS, RESTART_CONTAINER, UPDATE_CONFIG
    target_service VARCHAR(100) NOT NULL,
    parameters JSONB NOT NULL,                          -- e.g. {"rollback_to_version": "v1.0.0"}
    dry_run_passed BOOLEAN NOT NULL DEFAULT FALSE,
    policy_validator_notes TEXT,
    status VARCHAR(30) NOT NULL DEFAULT 'PENDING',      -- PENDING, APPROVED, EXECUTING, VERIFYING, VERIFIED, REVERTED, FAILED
    executed_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    rollback_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_remediation_incident ON remediation_audit_log (incident_id);

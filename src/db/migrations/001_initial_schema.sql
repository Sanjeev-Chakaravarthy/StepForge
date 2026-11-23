-- Create run_status enum
DO $$ BEGIN
    CREATE TYPE run_status AS ENUM (
        'PENDING',
        'QUEUED',
        'RUNNING',
        'RETRYING',
        'COMPLETED',
        'DEAD_LETTER'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Create outbox_status enum
DO $$ BEGIN
    CREATE TYPE outbox_status AS ENUM (
        'PENDING',
        'PUBLISHED',
        'FAILED'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Tenants Table
CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    max_concurrency INTEGER NOT NULL DEFAULT 5,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Workflow Runs Table
CREATE TABLE IF NOT EXISTS runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    workflow_name VARCHAR(128) NOT NULL,
    idempotency_key VARCHAR(255) NOT NULL,
    status run_status NOT NULL DEFAULT 'PENDING',
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    output JSONB NULL,
    error JSONB NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    current_lease_worker_id VARCHAR(128) NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_idempotency UNIQUE(tenant_id, idempotency_key)
);

-- Step Checkpoints Table (Durable Step-Level Memoization)
CREATE TABLE IF NOT EXISTS step_checkpoints (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    step_key VARCHAR(255) NOT NULL,
    output JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL DEFAULT 'COMPLETED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_run_step UNIQUE(run_id, step_key)
);

-- Transactional Outbox Events Table
CREATE TABLE IF NOT EXISTS outbox_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status outbox_status NOT NULL DEFAULT 'PENDING',
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ NULL
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_runs_tenant_status ON runs(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_runs_lease_reaper ON runs(status, lease_expires_at) 
    WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS idx_runs_scheduled_retrying ON runs(status, scheduled_at) 
    WHERE status = 'RETRYING';
CREATE INDEX IF NOT EXISTS idx_step_checkpoints_lookup ON step_checkpoints(run_id, step_key);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(available_at) 
    WHERE status = 'PENDING';

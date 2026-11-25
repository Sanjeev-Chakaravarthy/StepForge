-- Extend run_status enum with WAITING_HUMAN for HITL pause/resume
ALTER TYPE run_status ADD VALUE IF NOT EXISTS 'WAITING_HUMAN';

-- Human-in-the-Loop approval gates
CREATE TABLE IF NOT EXISTS run_approvals (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    gate_key VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED')),
    prompt_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ NULL,
    CONSTRAINT uq_run_approval_gate UNIQUE(run_id, gate_key)
);

CREATE INDEX IF NOT EXISTS idx_run_approvals_lookup ON run_approvals(run_id, gate_key);
CREATE INDEX IF NOT EXISTS idx_run_approvals_pending ON run_approvals(run_id, status)
    WHERE status = 'PENDING';

-- Extend step_checkpoints with step_type and structured metadata (latency, token usage, tool name)
ALTER TABLE step_checkpoints
    ADD COLUMN IF NOT EXISTS step_type VARCHAR(32) NOT NULL DEFAULT 'STEP',
    ADD COLUMN IF NOT EXISTS metadata JSONB NULL;

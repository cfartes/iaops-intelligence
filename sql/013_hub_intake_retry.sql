CREATE SCHEMA IF NOT EXISTS iaops_gov;

CREATE TABLE IF NOT EXISTS iaops_gov.hub_intake_pending (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL UNIQUE REFERENCES iaops_gov.client(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_status_code INTEGER,
    payload_json JSONB,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hub_intake_pending_status_next_retry
    ON iaops_gov.hub_intake_pending (status, next_retry_at);

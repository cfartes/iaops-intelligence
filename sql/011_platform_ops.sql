-- Extensoes de plataforma: LGPD, billing, RAG e jobs assinc
CREATE SCHEMA IF NOT EXISTS iaops_gov;

CREATE TABLE IF NOT EXISTS iaops_gov.lgpd_policy (
    tenant_id BIGINT PRIMARY KEY REFERENCES iaops_gov.tenant(id),
    dpo_name TEXT,
    dpo_email TEXT,
    retention_days INTEGER,
    legal_notes TEXT,
    updated_by_user_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE iaops_gov.lgpd_policy ADD COLUMN IF NOT EXISTS retention_days INTEGER;
ALTER TABLE iaops_gov.lgpd_policy ADD COLUMN IF NOT EXISTS updated_by_user_id BIGINT;
ALTER TABLE iaops_gov.lgpd_policy ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE iaops_gov.lgpd_policy ADD COLUMN IF NOT EXISTS legal_notes TEXT;

CREATE TABLE IF NOT EXISTS iaops_gov.lgpd_rule (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    rule_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_by_user_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE iaops_gov.lgpd_rule ADD COLUMN IF NOT EXISTS schema_name TEXT;
ALTER TABLE iaops_gov.lgpd_rule ADD COLUMN IF NOT EXISTS table_name TEXT;
ALTER TABLE iaops_gov.lgpd_rule ADD COLUMN IF NOT EXISTS column_name TEXT;
ALTER TABLE iaops_gov.lgpd_rule ADD COLUMN IF NOT EXISTS rule_type TEXT;
ALTER TABLE iaops_gov.lgpd_rule ADD COLUMN IF NOT EXISTS rule_config JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE iaops_gov.lgpd_rule ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
CREATE INDEX IF NOT EXISTS idx_lgpd_rule_tenant ON iaops_gov.lgpd_rule(tenant_id, is_active);

CREATE TABLE IF NOT EXISTS iaops_gov.lgpd_dsr_request (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    requester_name TEXT NOT NULL,
    requester_email TEXT NOT NULL,
    request_type TEXT NOT NULL,
    subject_key TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    notes TEXT,
    opened_by_user_id BIGINT,
    resolved_by_user_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_lgpd_dsr_tenant ON iaops_gov.lgpd_dsr_request(tenant_id, status);

CREATE TABLE IF NOT EXISTS iaops_gov.billing_plan (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    max_tenants INTEGER NOT NULL,
    max_users INTEGER NOT NULL,
    monthly_price_cents INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.billing_subscription (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL REFERENCES iaops_gov.client(id),
    plan_id BIGINT NOT NULL REFERENCES iaops_gov.billing_plan(id),
    status TEXT NOT NULL DEFAULT 'active',
    starts_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ends_at TIMESTAMPTZ,
    tolerance_days INTEGER NOT NULL DEFAULT 5,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_billing_sub_client ON iaops_gov.billing_subscription(client_id, status);

CREATE TABLE IF NOT EXISTS iaops_gov.billing_installment (
    id BIGSERIAL PRIMARY KEY,
    subscription_id BIGINT NOT NULL REFERENCES iaops_gov.billing_subscription(id),
    due_date DATE NOT NULL,
    amount_cents INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    paid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_billing_installment_sub ON iaops_gov.billing_installment(subscription_id, status, due_date);

CREATE TABLE IF NOT EXISTS iaops_gov.llm_usage_meter (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    feature_code TEXT NOT NULL,
    model_code TEXT,
    provider_name TEXT,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    amount_cents INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_llm_usage_tenant ON iaops_gov.llm_usage_meter(tenant_id, created_at);

CREATE TABLE IF NOT EXISTS iaops_gov.rag_document (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    data_source_id BIGINT REFERENCES iaops_gov.data_source(id),
    doc_kind TEXT NOT NULL,
    doc_key TEXT NOT NULL,
    content_text TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    embedding_json JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, doc_kind, doc_key)
);
CREATE INDEX IF NOT EXISTS idx_rag_document_tenant ON iaops_gov.rag_document(tenant_id, doc_kind);

CREATE TABLE IF NOT EXISTS iaops_gov.async_job_run (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT REFERENCES iaops_gov.tenant(id),
    job_kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB,
    error_text TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_async_job_run_tenant ON iaops_gov.async_job_run(tenant_id, status, created_at);

INSERT INTO iaops_gov.billing_plan (code, name, max_tenants, max_users, monthly_price_cents, is_active)
VALUES
    ('starter', 'Starter', 1, 5, 29900, TRUE),
    ('pro', 'Pro', 5, 50, 99900, TRUE),
    ('enterprise', 'Enterprise', 999, 9999, 0, TRUE)
ON CONFLICT (code) DO NOTHING;

CREATE SCHEMA IF NOT EXISTS iaops_gov;

CREATE TABLE IF NOT EXISTS iaops_gov.plan (
    id BIGSERIAL PRIMARY KEY,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    max_tenants INTEGER NOT NULL,
    max_users INTEGER NOT NULL,
    monthly_price_cents BIGINT NOT NULL,
    late_tolerance_days INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.client (
    id BIGSERIAL PRIMARY KEY,
    fantasy_name TEXT NOT NULL,
    legal_name TEXT NOT NULL,
    cnpj TEXT NOT NULL UNIQUE,
    address_text TEXT NOT NULL,
    contact_phone TEXT NOT NULL,
    contact_email TEXT NOT NULL,
    access_email TEXT NOT NULL,
    notification_email TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    email_confirmed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.subscription (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL REFERENCES iaops_gov.client(id),
    plan_id BIGINT NOT NULL REFERENCES iaops_gov.plan(id),
    starts_at TIMESTAMPTZ NOT NULL,
    ends_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.tenant (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL REFERENCES iaops_gov.client(id),
    name TEXT NOT NULL,
    slug TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (client_id, slug)
);

CREATE TABLE IF NOT EXISTS iaops_gov.app_user (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL REFERENCES iaops_gov.client(id),
    email TEXT NOT NULL,
    full_name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (client_id, email)
);

CREATE TABLE IF NOT EXISTS iaops_gov.tenant_user_role (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    user_id BIGINT NOT NULL REFERENCES iaops_gov.app_user(id),
    role TEXT NOT NULL CHECK (role IN ('viewer', 'admin', 'owner')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS iaops_gov.user_tenant_preference (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    user_id BIGINT NOT NULL REFERENCES iaops_gov.app_user(id),
    language_code TEXT NOT NULL DEFAULT 'pt-BR',
    theme_code TEXT NOT NULL DEFAULT 'light',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS iaops_gov.data_source (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    source_type TEXT NOT NULL,
    conn_secret_ref TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.monitored_table (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    data_source_id BIGINT NOT NULL REFERENCES iaops_gov.data_source(id),
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, data_source_id, schema_name, table_name)
);

CREATE TABLE IF NOT EXISTS iaops_gov.monitored_column (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    monitored_table_id BIGINT NOT NULL REFERENCES iaops_gov.monitored_table(id),
    column_name TEXT NOT NULL,
    data_type TEXT,
    classification TEXT,
    description_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (monitored_table_id, column_name)
);

CREATE TABLE IF NOT EXISTS iaops_gov.schema_change_event (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    monitored_table_id BIGINT NOT NULL REFERENCES iaops_gov.monitored_table(id),
    change_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.incident (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    source_event_id BIGINT REFERENCES iaops_gov.schema_change_event(id),
    title TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'ack', 'resolved', 'closed')),
    severity TEXT NOT NULL,
    sla_due_at TIMESTAMPTZ,
    ack_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.lgpd_policy (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL UNIQUE REFERENCES iaops_gov.tenant(id),
    dpo_name TEXT,
    dpo_email TEXT,
    retention_policy_text TEXT,
    legal_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.lgpd_rule (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    monitored_column_id BIGINT REFERENCES iaops_gov.monitored_column(id),
    rule_name TEXT NOT NULL,
    rule_expression TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.data_subject_request (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    request_type TEXT NOT NULL,
    requester_email TEXT NOT NULL,
    status TEXT NOT NULL,
    protocol_code TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.llm_provider (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT REFERENCES iaops_gov.client(id),
    name TEXT NOT NULL,
    model_code TEXT NOT NULL,
    endpoint_url TEXT,
    secret_ref TEXT,
    is_global_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.tenant_llm_config (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL UNIQUE REFERENCES iaops_gov.tenant(id),
    llm_provider_id BIGINT NOT NULL REFERENCES iaops_gov.llm_provider(id),
    billing_mode TEXT NOT NULL CHECK (billing_mode IN ('tenant_provider', 'app_default_token')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.chat_session (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    user_id BIGINT NOT NULL REFERENCES iaops_gov.app_user(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.chat_message (
    id BIGSERIAL PRIMARY KEY,
    session_id BIGINT NOT NULL REFERENCES iaops_gov.chat_session(id),
    sender_type TEXT NOT NULL CHECK (sender_type IN ('user', 'assistant', 'system')),
    content_text TEXT NOT NULL,
    sql_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.token_usage (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    llm_provider_id BIGINT NOT NULL REFERENCES iaops_gov.llm_provider(id),
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    unit_price_per_1k_cents BIGINT NOT NULL DEFAULT 0,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.invoice (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL REFERENCES iaops_gov.client(id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    total_cents BIGINT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS iaops_gov.installment (
    id BIGSERIAL PRIMARY KEY,
    invoice_id BIGINT NOT NULL REFERENCES iaops_gov.invoice(id),
    sequence_number INTEGER NOT NULL,
    due_date DATE NOT NULL,
    amount_cents BIGINT NOT NULL,
    paid_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (invoice_id, sequence_number)
);

CREATE TABLE IF NOT EXISTS iaops_gov.notification_channel (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'whatsapp')),
    config_secret_ref TEXT NOT NULL,
    is_healthy BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, channel_type)
);

CREATE TABLE IF NOT EXISTS iaops_gov.audit_log (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT REFERENCES iaops_gov.client(id),
    tenant_id BIGINT REFERENCES iaops_gov.tenant(id),
    user_id BIGINT REFERENCES iaops_gov.app_user(id),
    action_code TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT,
    payload_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tenant_table_tenant_id ON iaops_gov.tenant(client_id);
CREATE INDEX IF NOT EXISTS idx_monitored_table_tenant ON iaops_gov.monitored_table(tenant_id);
CREATE INDEX IF NOT EXISTS idx_event_tenant_detected ON iaops_gov.schema_change_event(tenant_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_incident_tenant_status ON iaops_gov.incident(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_token_usage_tenant_consumed ON iaops_gov.token_usage(tenant_id, consumed_at DESC);
CREATE INDEX IF NOT EXISTS idx_installment_due_status ON iaops_gov.installment(due_date, status);
CREATE INDEX IF NOT EXISTS idx_audit_tenant_created ON iaops_gov.audit_log(tenant_id, created_at DESC);
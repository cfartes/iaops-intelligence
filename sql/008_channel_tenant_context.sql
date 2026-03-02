CREATE TABLE IF NOT EXISTS iaops_gov.channel_user_binding (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL REFERENCES iaops_gov.client(id),
    user_id BIGINT NOT NULL REFERENCES iaops_gov.app_user(id),
    channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'whatsapp')),
    external_user_key TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (channel_type, external_user_key)
);

CREATE TABLE IF NOT EXISTS iaops_gov.channel_tenant_context (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL REFERENCES iaops_gov.client(id),
    user_id BIGINT NOT NULL REFERENCES iaops_gov.app_user(id),
    channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'whatsapp')),
    conversation_key TEXT NOT NULL,
    active_tenant_id BIGINT REFERENCES iaops_gov.tenant(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (channel_type, conversation_key)
);

CREATE INDEX IF NOT EXISTS idx_channel_binding_client_user
ON iaops_gov.channel_user_binding(client_id, user_id);

CREATE INDEX IF NOT EXISTS idx_channel_context_client_user
ON iaops_gov.channel_tenant_context(client_id, user_id);

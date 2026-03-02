CREATE TABLE IF NOT EXISTS iaops_gov.mcp_server (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT REFERENCES iaops_gov.client(id),
    name TEXT NOT NULL,
    transport_type TEXT NOT NULL CHECK (transport_type IN ('stdio', 'http', 'websocket')),
    endpoint_url TEXT,
    is_internal BOOLEAN NOT NULL DEFAULT TRUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (client_id, name)
);

CREATE TABLE IF NOT EXISTS iaops_gov.mcp_tool (
    id BIGSERIAL PRIMARY KEY,
    mcp_server_id BIGINT NOT NULL REFERENCES iaops_gov.mcp_server(id),
    tool_name TEXT NOT NULL,
    tool_version TEXT NOT NULL DEFAULT 'v1',
    min_role TEXT NOT NULL CHECK (min_role IN ('viewer', 'admin', 'owner')),
    risk_level TEXT NOT NULL DEFAULT 'medium' CHECK (risk_level IN ('low', 'medium', 'high')),
    timeout_ms INTEGER NOT NULL DEFAULT 15000,
    input_schema_json JSONB,
    output_schema_json JSONB,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (mcp_server_id, tool_name, tool_version)
);

CREATE TABLE IF NOT EXISTS iaops_gov.tenant_mcp_tool_policy (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    mcp_tool_id BIGINT NOT NULL REFERENCES iaops_gov.mcp_tool(id),
    is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    max_rows INTEGER,
    max_calls_per_minute INTEGER,
    require_masking BOOLEAN NOT NULL DEFAULT TRUE,
    allowed_schema_patterns JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, mcp_tool_id)
);

CREATE TABLE IF NOT EXISTS iaops_gov.mcp_client_connection (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    connection_name TEXT NOT NULL,
    transport_type TEXT NOT NULL CHECK (transport_type IN ('stdio', 'http', 'websocket')),
    endpoint_url TEXT,
    auth_secret_ref TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    health_status TEXT NOT NULL DEFAULT 'unknown' CHECK (health_status IN ('unknown', 'healthy', 'degraded', 'down')),
    last_healthcheck_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, connection_name)
);

CREATE TABLE IF NOT EXISTS iaops_gov.mcp_call_log (
    id BIGSERIAL PRIMARY KEY,
    client_id BIGINT REFERENCES iaops_gov.client(id),
    tenant_id BIGINT REFERENCES iaops_gov.tenant(id),
    user_id BIGINT REFERENCES iaops_gov.app_user(id),
    mcp_server_id BIGINT REFERENCES iaops_gov.mcp_server(id),
    mcp_tool_id BIGINT REFERENCES iaops_gov.mcp_tool(id),
    direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    correlation_id TEXT NOT NULL,
    request_payload_json JSONB,
    response_payload_json JSONB,
    status TEXT NOT NULL CHECK (status IN ('success', 'denied', 'error', 'timeout')),
    error_code TEXT,
    error_message TEXT,
    latency_ms INTEGER,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS iaops_gov.mcp_token_usage (
    id BIGSERIAL PRIMARY KEY,
    mcp_call_log_id BIGINT NOT NULL REFERENCES iaops_gov.mcp_call_log(id),
    tenant_id BIGINT NOT NULL REFERENCES iaops_gov.tenant(id),
    llm_provider_id BIGINT REFERENCES iaops_gov.llm_provider(id),
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    unit_price_per_1k_cents BIGINT NOT NULL DEFAULT 0,
    total_cost_cents BIGINT NOT NULL DEFAULT 0,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mcp_tool_server ON iaops_gov.mcp_tool(mcp_server_id);
CREATE INDEX IF NOT EXISTS idx_tenant_mcp_policy_tenant ON iaops_gov.tenant_mcp_tool_policy(tenant_id);
CREATE INDEX IF NOT EXISTS idx_mcp_call_log_tenant_requested ON iaops_gov.mcp_call_log(tenant_id, requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_mcp_call_log_status_requested ON iaops_gov.mcp_call_log(status, requested_at DESC);
CREATE INDEX IF NOT EXISTS idx_mcp_call_log_correlation ON iaops_gov.mcp_call_log(correlation_id);
CREATE INDEX IF NOT EXISTS idx_mcp_token_usage_tenant_consumed ON iaops_gov.mcp_token_usage(tenant_id, consumed_at DESC);
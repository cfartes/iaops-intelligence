-- Demo data baseline para desenvolvimento local
-- Requer execucao previa de:
-- 001_initial_governance_schema.sql
-- 002_mcp_foundation.sql
-- 003_mcp_seed.sql

INSERT INTO iaops_gov.plan (
    id,
    code,
    name,
    max_tenants,
    max_users,
    monthly_price_cents,
    late_tolerance_days,
    created_at
)
VALUES (
    1,
    'starter',
    'Starter',
    5,
    50,
    19900,
    10,
    NOW()
)
ON CONFLICT (id) DO UPDATE SET
    code = EXCLUDED.code,
    name = EXCLUDED.name,
    max_tenants = EXCLUDED.max_tenants,
    max_users = EXCLUDED.max_users,
    monthly_price_cents = EXCLUDED.monthly_price_cents,
    late_tolerance_days = EXCLUDED.late_tolerance_days;

INSERT INTO iaops_gov.client (
    id,
    fantasy_name,
    legal_name,
    cnpj,
    address_text,
    contact_phone,
    contact_email,
    access_email,
    notification_email,
    password_hash,
    email_confirmed_at,
    status,
    created_at
)
VALUES (
    1,
    'IAOps Demo',
    'IAOps Demo LTDA',
    '12345678000199',
    'Rua Exemplo, 100 - Sao Paulo/SP',
    '+55 11 90000-0000',
    'contato@iaops.demo',
    'owner@iaops.demo',
    'alerts@iaops.demo',
    'demo_hash_not_for_prod',
    NOW(),
    'active',
    NOW()
)
ON CONFLICT (id) DO UPDATE SET
    fantasy_name = EXCLUDED.fantasy_name,
    legal_name = EXCLUDED.legal_name,
    cnpj = EXCLUDED.cnpj,
    address_text = EXCLUDED.address_text,
    contact_phone = EXCLUDED.contact_phone,
    contact_email = EXCLUDED.contact_email,
    access_email = EXCLUDED.access_email,
    notification_email = EXCLUDED.notification_email,
    status = EXCLUDED.status,
    email_confirmed_at = COALESCE(iaops_gov.client.email_confirmed_at, EXCLUDED.email_confirmed_at);

INSERT INTO iaops_gov.subscription (
    id,
    client_id,
    plan_id,
    starts_at,
    ends_at,
    status,
    created_at
)
VALUES (
    1,
    1,
    1,
    NOW() - INTERVAL '30 days',
    NULL,
    'active',
    NOW()
)
ON CONFLICT (id) DO UPDATE SET
    client_id = EXCLUDED.client_id,
    plan_id = EXCLUDED.plan_id,
    starts_at = EXCLUDED.starts_at,
    ends_at = EXCLUDED.ends_at,
    status = EXCLUDED.status;

INSERT INTO iaops_gov.tenant (
    id,
    client_id,
    name,
    slug,
    status,
    created_at
)
VALUES (
    10,
    1,
    'Tenant Demo',
    'tenant-demo',
    'active',
    NOW()
)
ON CONFLICT (id) DO UPDATE SET
    client_id = EXCLUDED.client_id,
    name = EXCLUDED.name,
    slug = EXCLUDED.slug,
    status = EXCLUDED.status;

INSERT INTO iaops_gov.app_user (
    id,
    client_id,
    email,
    full_name,
    password_hash,
    is_active,
    created_at
)
VALUES
    (100, 1, 'owner@iaops.demo', 'Owner Demo', 'demo_hash_not_for_prod', TRUE, NOW()),
    (101, 1, 'admin@iaops.demo', 'Admin Demo', 'demo_hash_not_for_prod', TRUE, NOW()),
    (102, 1, 'viewer@iaops.demo', 'Viewer Demo', 'demo_hash_not_for_prod', TRUE, NOW())
ON CONFLICT (id) DO UPDATE SET
    client_id = EXCLUDED.client_id,
    email = EXCLUDED.email,
    full_name = EXCLUDED.full_name,
    is_active = EXCLUDED.is_active;

INSERT INTO iaops_gov.tenant_user_role (
    tenant_id,
    user_id,
    role,
    created_at
)
VALUES
    (10, 100, 'owner', NOW()),
    (10, 101, 'admin', NOW()),
    (10, 102, 'viewer', NOW())
ON CONFLICT (tenant_id, user_id) DO UPDATE SET
    role = EXCLUDED.role;

INSERT INTO iaops_gov.user_tenant_preference (
    tenant_id,
    user_id,
    language_code,
    theme_code,
    created_at
)
VALUES
    (10, 100, 'pt-BR', 'light', NOW()),
    (10, 101, 'pt-BR', 'light', NOW()),
    (10, 102, 'pt-BR', 'light', NOW())
ON CONFLICT (tenant_id, user_id) DO UPDATE SET
    language_code = EXCLUDED.language_code,
    theme_code = EXCLUDED.theme_code;

INSERT INTO iaops_gov.data_source (
    id,
    tenant_id,
    source_type,
    conn_secret_ref,
    is_active,
    created_at
)
VALUES (
    1000,
    10,
    'postgres',
    'secret://tenant-10/source-main',
    TRUE,
    NOW()
)
ON CONFLICT (id) DO UPDATE SET
    tenant_id = EXCLUDED.tenant_id,
    source_type = EXCLUDED.source_type,
    conn_secret_ref = EXCLUDED.conn_secret_ref,
    is_active = EXCLUDED.is_active;

INSERT INTO iaops_gov.monitored_table (
    id,
    tenant_id,
    data_source_id,
    schema_name,
    table_name,
    is_active,
    created_at
)
VALUES
    (2001, 10, 1000, 'public', 'orders', TRUE, NOW()),
    (2002, 10, 1000, 'public', 'customers', TRUE, NOW()),
    (2003, 10, 1000, 'analytics', 'kpi_daily', TRUE, NOW())
ON CONFLICT (id) DO UPDATE SET
    tenant_id = EXCLUDED.tenant_id,
    data_source_id = EXCLUDED.data_source_id,
    schema_name = EXCLUDED.schema_name,
    table_name = EXCLUDED.table_name,
    is_active = EXCLUDED.is_active;

INSERT INTO iaops_gov.monitored_column (
    tenant_id,
    monitored_table_id,
    column_name,
    data_type,
    classification,
    description_text,
    created_at
)
VALUES
    (10, 2001, 'id', 'bigint', 'identifier', 'Chave primaria do pedido', NOW()),
    (10, 2001, 'customer_cpf', 'text', 'sensitive', 'CPF do cliente', NOW()),
    (10, 2001, 'total_amount', 'numeric', 'financial', 'Valor total do pedido', NOW()),
    (10, 2002, 'id', 'bigint', 'identifier', 'Chave primaria do cliente', NOW()),
    (10, 2002, 'name', 'text', 'personal_data', 'Nome do cliente', NOW()),
    (10, 2003, 'ref_date', 'date', 'operational', 'Data de referencia', NOW()),
    (10, 2003, 'gross_revenue', 'numeric', 'financial', 'Receita bruta diaria', NOW())
ON CONFLICT (monitored_table_id, column_name) DO UPDATE SET
    data_type = EXCLUDED.data_type,
    classification = EXCLUDED.classification,
    description_text = EXCLUDED.description_text;

INSERT INTO iaops_gov.schema_change_event (
    tenant_id,
    monitored_table_id,
    change_type,
    severity,
    payload_json,
    detected_at
)
VALUES
    (10, 2001, 'column_type_changed', 'critical', '{"column":"customer_cpf","from":"varchar(11)","to":"text"}', NOW() - INTERVAL '20 minutes'),
    (10, 2003, 'column_added', 'medium', '{"column":"net_revenue","data_type":"numeric"}', NOW() - INTERVAL '45 minutes');

INSERT INTO iaops_gov.incident (
    tenant_id,
    source_event_id,
    title,
    status,
    severity,
    sla_due_at,
    ack_at,
    resolved_at,
    closed_at,
    created_at
)
VALUES
    (10, NULL, 'Divergencia de tipo em customer_cpf', 'open', 'high', NOW() + INTERVAL '4 hours', NULL, NULL, NULL, NOW() - INTERVAL '30 minutes'),
    (10, NULL, 'Ajuste em kpi_daily validado', 'ack', 'medium', NOW() + INTERVAL '12 hours', NOW() - INTERVAL '10 minutes', NULL, NULL, NOW() - INTERVAL '20 minutes');

INSERT INTO iaops_gov.mcp_client_connection (
    tenant_id,
    connection_name,
    transport_type,
    endpoint_url,
    auth_secret_ref,
    is_active,
    health_status,
    last_healthcheck_at,
    created_at
)
VALUES
    (10, 'telegram-alerts', 'http', 'https://api.telegram.org', 'secret://tenant-10/telegram', TRUE, 'healthy', NOW(), NOW()),
    (10, 'whatsapp-alerts', 'http', 'https://graph.facebook.com', 'secret://tenant-10/whatsapp', TRUE, 'degraded', NOW(), NOW())
ON CONFLICT (tenant_id, connection_name) DO UPDATE SET
    transport_type = EXCLUDED.transport_type,
    endpoint_url = EXCLUDED.endpoint_url,
    auth_secret_ref = EXCLUDED.auth_secret_ref,
    is_active = EXCLUDED.is_active,
    health_status = EXCLUDED.health_status,
    last_healthcheck_at = EXCLUDED.last_healthcheck_at;

INSERT INTO iaops_gov.notification_channel (
    tenant_id,
    channel_type,
    config_secret_ref,
    is_healthy,
    updated_at
)
VALUES
    (10, 'telegram', 'secret://tenant-10/telegram', TRUE, NOW()),
    (10, 'whatsapp', 'secret://tenant-10/whatsapp', FALSE, NOW())
ON CONFLICT (tenant_id, channel_type) DO UPDATE SET
    config_secret_ref = EXCLUDED.config_secret_ref,
    is_healthy = EXCLUDED.is_healthy,
    updated_at = EXCLUDED.updated_at;

SELECT setval(pg_get_serial_sequence('iaops_gov.plan', 'id'), GREATEST((SELECT COALESCE(MAX(id), 1) FROM iaops_gov.plan), 1), TRUE);
SELECT setval(pg_get_serial_sequence('iaops_gov.client', 'id'), GREATEST((SELECT COALESCE(MAX(id), 1) FROM iaops_gov.client), 1), TRUE);
SELECT setval(pg_get_serial_sequence('iaops_gov.subscription', 'id'), GREATEST((SELECT COALESCE(MAX(id), 1) FROM iaops_gov.subscription), 1), TRUE);
SELECT setval(pg_get_serial_sequence('iaops_gov.tenant', 'id'), GREATEST((SELECT COALESCE(MAX(id), 1) FROM iaops_gov.tenant), 1), TRUE);
SELECT setval(pg_get_serial_sequence('iaops_gov.app_user', 'id'), GREATEST((SELECT COALESCE(MAX(id), 1) FROM iaops_gov.app_user), 1), TRUE);
SELECT setval(pg_get_serial_sequence('iaops_gov.data_source', 'id'), GREATEST((SELECT COALESCE(MAX(id), 1) FROM iaops_gov.data_source), 1), TRUE);
SELECT setval(pg_get_serial_sequence('iaops_gov.monitored_table', 'id'), GREATEST((SELECT COALESCE(MAX(id), 1) FROM iaops_gov.monitored_table), 1), TRUE);

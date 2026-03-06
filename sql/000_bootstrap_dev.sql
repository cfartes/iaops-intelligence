-- Bootstrap unico para ambiente local de desenvolvimento
-- Execute no psql a partir da raiz do repositorio:
-- \i sql/000_bootstrap_dev.sql

\i sql/001_initial_governance_schema.sql
\i sql/002_mcp_foundation.sql
\i sql/005_data_source_catalog.sql
\i sql/006_user_mfa.sql
\i sql/007_superadmin_user.sql
\i sql/008_channel_tenant_context.sql
\i sql/009_user_tenant_chat_mode.sql
\i sql/010_client_signup.sql
\i sql/011_platform_ops.sql
\i sql/012_billing_consistency.sql
\i sql/013_hub_intake_retry.sql
\i sql/003_mcp_seed.sql
\i sql/004_demo_data.sql

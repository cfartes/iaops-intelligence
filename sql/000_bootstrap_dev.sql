-- Bootstrap unico para ambiente local de desenvolvimento
-- Execute no psql a partir da raiz do repositorio:
-- \i sql/000_bootstrap_dev.sql

\i sql/001_initial_governance_schema.sql
\i sql/002_mcp_foundation.sql
\i sql/005_data_source_catalog.sql
\i sql/003_mcp_seed.sql
\i sql/004_demo_data.sql
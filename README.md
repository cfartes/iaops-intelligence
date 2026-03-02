# IAOps Governance

Plataforma SaaS multi-tenant para governanca de dados, monitoramento operacional, seguranca e conformidade LGPD.

## Conteudo
- [Visao de produto](docs/product/vision-and-scope.md)
- [Requisitos funcionais e regras](docs/product/functional-requirements.md)
- [Modelo de dominio e dados](docs/architecture/domain-model.md)
- [Arquitetura MCP](docs/architecture/mcp-integration.md)
- [Backlog MVP](docs/product/mvp-backlog.md)
- [DDL inicial PostgreSQL](sql/001_initial_governance_schema.sql)
- [DDL base MCP](sql/002_mcp_foundation.sql)
- [Catalogo de fontes](sql/005_data_source_catalog.sql)
- [MFA por usuario (TOTP)](sql/006_user_mfa.sql)
- [Flag de superadmin](sql/007_superadmin_user.sql)
- [Contexto de tenant por canal](sql/008_channel_tenant_context.sql)
- [Extensoes de plataforma (LGPD/Billing/RAG/Jobs)](sql/011_platform_ops.sql)
- [Seed MCP baseline](sql/003_mcp_seed.sql)
- [Dados demo locais](sql/004_demo_data.sql)
- [Bootstrap dev unico](sql/000_bootstrap_dev.sql)
- [Frontend Node](frontend/README.md)

## Stack alvo
- Frontend: Node.js (React + Vite)
- Backend logico: Python (`functions.py` + API HTTP)
- Banco: PostgreSQL
- Assincrono: Celery + Redis
- Integracoes: Telegram, WhatsApp Cloud API, SMTP
- Infra: Docker Compose em VPS

## Padrao de interface definido
- Navegacao principal via menu lateral.
- Todo cadastro em tela modal.
- Toda mensagem de sistema (alerta, aviso, erro, confirmacao) em tela modal.

## MCP Scaffold (Python)
- Entrypoint logico: `functions.py` (`handle_request(payload)`)
- API HTTP: `iaops/api/server.py`
- Gateway: `iaops/mcp/gateway.py`
- Contratos/modelos: `iaops/mcp/models.py`
- Repositorio inicial (mock): `iaops/mcp/repository.py`
- Repositorio PostgreSQL: `iaops/mcp/postgres_repository.py`

### Tools stub publicadas
- `access.list_users`
- `security.mfa.get_status`
- `security.mfa.begin_setup`
- `security.mfa.enable`
- `security.mfa.disable_self`
- `security.mfa.admin_reset`
- `tenant.list_client`
- `tenant.get_limits`
- `tenant.create`
- `tenant.update_status`
- `tenant_llm.list_providers`
- `tenant_llm.get_config`
- `tenant_llm.update_config`
- `llm_admin.list_providers`
- `llm_admin.get_app_config`
- `llm_admin.update_app_config`
- `channel.list_user_tenants`
- `channel.set_active_tenant`
- `channel.get_active_tenant`
- `source.list_catalog`
- `source.list_tenant`
- `source.register`
- `source.update_status`
- `source.update`
- `source.delete`
- `inventory.list_tables`
- `inventory.list_columns`
- `inventory.list_tenant_tables`
- `inventory.register_table`
- `inventory.delete_table`
- `inventory.list_table_columns`
- `inventory.register_column`
- `inventory.delete_column`
- `query.execute_safe_sql`
- `security_sql.get_policy`
- `security_sql.update_policy`
- `incident.create`
- `incident.list`
- `incident.update_status`
- `events.list`
- `audit.list_calls`
- `ops.get_health_summary`

Regras de transicao de incidente:
- `open -> ack/resolved`
- `ack -> resolved/closed`
- `resolved -> closed`
- `closed` sem transicoes posteriores

## Execucao local
Instalar dependencias Python:
```powershell
pip install -r requirements.txt
```

Para usar persistencia PostgreSQL em vez de memoria:
```powershell
$env:IAOPS_DB_DSN="postgresql://usuario:senha@host:5432/banco"
$env:IAOPS_DB_SCHEMA="iaops_gov"
```

Aplicar tudo com bootstrap unico:
```sql
\i sql/000_bootstrap_dev.sql
```

Ou aplicar scripts SQL manualmente:
```sql
\i sql/001_initial_governance_schema.sql
\i sql/002_mcp_foundation.sql
\i sql/005_data_source_catalog.sql
\i sql/006_user_mfa.sql
\i sql/007_superadmin_user.sql
\i sql/008_channel_tenant_context.sql
\i sql/011_platform_ops.sql
\i sql/003_mcp_seed.sql
\i sql/004_demo_data.sql
```

Se voce ja havia rodado o seed antes, execute novamente `003_mcp_seed.sql` para incluir as tools novas (catalogo, auditoria, seguranca SQL e afins).

Para criptografia de segredos (incluindo MFA) em producao:
```powershell
$env:IAOPS_CRYPTO_KEY="<fernet_key_urlsafe_base64>"
```

Preco por 1k token ao usar LLM padrao do app (fallback):
```powershell
$env:IAOPS_APP_LLM_PRICE_PER_1K_CENTS="50"
```

Backend API (porta 8000):
```powershell
python -m iaops.api.server
```

Frontend (porta 5173):
```powershell
cd frontend
npm install
npm run dev
```

## Deploy em Docker (VPS Ubuntu 24.04 LTS)
Arquivos adicionados:
- `docker-compose.yml`
- `docker/backend.Dockerfile`
- `docker/frontend.Dockerfile`
- `docker/nginx.conf`
- `.env.example`

Passos:
```bash
cp .env.example .env
docker compose up -d --build
```

Servicos:
- Frontend: `http://SEU_HOST:5173`
- API: `http://SEU_HOST:8000`
- PostgreSQL: `SEU_HOST:5432`
- Redis: `SEU_HOST:6379`
- Worker Celery: `iaops-worker` (interno no compose)

Observacoes de producao:
- Configure `IAOPS_CRYPTO_KEY` no `.env` (Fernet key valida).
- Configure SMTP (`IAOPS_SMTP_*`) para confirmacao de cadastro e reset de senha.
- O backend instala `msodbcsql18`, `pyodbc`, `pymysql` e `oracledb` para testes autenticados de conectores.
- Jobs assinc:
  - `POST /api/jobs/ingest-metadata`
  - `POST /api/jobs/rag-rebuild`
  - `POST /api/jobs/monitor-scan`
  - `POST /api/jobs/billing-cycle`
  - `GET /api/jobs`

## Objetivo
Este repositorio inicia com baseline de produto e arquitetura para acelerar implementacao por iteracoes.

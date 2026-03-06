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
- [Consistencia de billing (view unificada)](sql/012_billing_consistency.sql)
- [Fila de retry do intake HUB](sql/013_hub_intake_retry.sql)
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
- `security_mcp.list_policies`
- `security_mcp.update_policy`
- `mcp_client.list_connections`
- `mcp_client.upsert_connection`
- `mcp_client.update_status`
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
\i sql/012_billing_consistency.sql
\i sql/013_hub_intake_retry.sql
\i sql/003_mcp_seed.sql
\i sql/004_demo_data.sql
```

Se voce ja havia rodado o seed antes, execute novamente `003_mcp_seed.sql` para incluir as tools novas (catalogo, auditoria, seguranca SQL e afins).

Para criptografia de segredos (incluindo MFA) em producao:
```powershell
$env:IAOPS_CRYPTO_KEY="<fernet_key_urlsafe_base64>"
```

Integracao com HUB Faturamento (intake de clientes):
```powershell
$env:IAOPS_HUB_BASE_URL="http://hub.local"
# opcional (sobrescreve URL calculada por IAOPS_HUB_BASE_URL):
$env:IAOPS_HUB_INTAKE_URL="http://hub.local/api/hub/intake/client-upsert"
# opcional via env (tambem pode ser salva em Configuracao > Superadmin):
$env:IAOPS_HUB_INTAKE_API_KEY="<api_key_intake_do_app_no_hub>"
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

Testes E2E (Playwright):
```powershell
cd frontend
npm install
npx playwright install
npm run e2e
```

Testes automatizados (backend):
```powershell
python -m unittest discover -s tests -p "test_*.py"
```

## Deploy em Docker (VPS Ubuntu 24.04 LTS)
Arquivos adicionados:
- `docker-compose.yml`
- `docker-compose.prod.yml`
- `docker-compose.ports.yml`
- `docker/backend.Dockerfile`
- `docker/frontend.Dockerfile`
- `docker/nginx.conf`
- `docker/Caddyfile`
- `.env.example`
- `.env.prod.example`
- `deploy_prod_vps.sh`

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
  - `POST /api/jobs/housekeeping`
  - `GET /api/jobs`

### Deploy de producao com dominio (HTTPS automatico)
Para VPS Ubuntu 24.04, usando dominio `iaops.nexusdataanalytics.tech`:

```bash
cp .env.prod.example .env.prod
# ajuste senhas/SMTP/chaves no .env.prod
chmod +x deploy_prod_vps.sh
./deploy_prod_vps.sh
```

O script:
- sobe stack com `docker compose` + `docker-compose.prod.yml` e, em modo proxy externo, inclui `docker-compose.ports.yml`
- por padrao usa **proxy externo** (multi-app), publicando somente `127.0.0.1:${IAOPS_FRONTEND_BIND_PORT:-18080}`
- opcionalmente publica via Caddy em `80/443` com TLS automatico se `IAOPS_USE_EMBEDDED_EDGE=1`
- aplica politica para manter apenas o usuario:
  - `superadmin@iaops.local`
  - senha: `AndradeFartes@2026!`

Modo recomendado para VPS com varios apps:
- manter `IAOPS_USE_EMBEDDED_EDGE=0`
- no proxy central (nginx/traefik/caddy do host), rotear:
  - `iaops.nexusdataanalytics.tech` -> `http://127.0.0.1:18080`

## Objetivo
Este repositorio inicia com baseline de produto e arquitetura para acelerar implementacao por iteracoes.

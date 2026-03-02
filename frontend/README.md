# Frontend (Node + React)

Frontend base do IAOps Governance com:
- menu lateral como navegacao principal;
- cadastro sempre em modal;
- mensagens do sistema sempre em modal.

## Requisitos
- Node.js 20+

## Executar
```bash
cd frontend
npm install
npm run dev
```

## Integracao backend MCP
O frontend consome os endpoints HTTP do backend Python via proxy do Vite:
- `GET /api/data-sources/catalog`
- `GET /api/data-sources`
- `GET /api/onboarding/monitored-tables`
- `GET /api/onboarding/monitored-columns`
- `GET /api/access/users`
- `GET /api/security/mfa/status`
- `GET /api/tenants`
- `GET /api/tenants/limits`
- `GET /api/admin/llm/providers`
- `GET /api/admin/llm/config`
- `GET /api/tenant-llm/providers`
- `GET /api/tenant-llm/config`
- `GET /api/inventory/tables`
- `GET /api/inventory/columns`
- `GET /api/events`
- `GET /api/incidents`
- `GET /api/operation/health`
- `GET /api/audit/calls`
- `GET /api/security-sql/policy`
- `POST /api/incidents`
- `POST /api/data-sources`
- `POST /api/data-sources/status`
- `POST /api/data-sources/update`
- `POST /api/data-sources/delete`
- `POST /api/onboarding/monitored-tables`
- `POST /api/onboarding/monitored-tables/delete`
- `POST /api/onboarding/monitored-columns`
- `POST /api/onboarding/monitored-columns/delete`
- `POST /api/security/mfa/setup`
- `POST /api/security/mfa/enable`
- `POST /api/security/mfa/disable`
- `POST /api/security/mfa/admin-reset`
- `POST /api/tenants`
- `POST /api/tenants/status`
- `POST /api/admin/llm/config`
- `POST /api/tenant-llm/config`
- `POST /api/channel/tenants/list`
- `POST /api/channel/tenant/select`
- `POST /api/channel/tenant/active`
- `POST /api/incidents/status`
- `POST /api/security-sql/policy`
- `POST /api/chat-bi/query`

Padrao de cabecalhos de contexto enviado:
- `X-Client-Id`
- `X-Tenant-Id`
- `X-User-Id`

## Estrutura
- `src/App.jsx`: shell principal e orquestracao dos modais
- `src/components/SideMenu.jsx`: menu lateral
- `src/components/EntityFormModal.jsx`: modal de cadastro
- `src/components/IncidentFormModal.jsx`: modal de abertura de incidente
- `src/components/IncidentStatusModal.jsx`: modal de atualizacao de status do incidente
- `src/components/SqlSecurityPolicyModal.jsx`: modal de edicao da politica SQL
- `src/components/SystemMessageModal.jsx`: modal de mensagens do sistema
- `src/pages/InventoryPanel.jsx`: inventario conectado ao MCP
- `src/pages/OnboardingPanel.jsx`: catalogo de fontes suportadas (inclui Power BI e Fabric)
- `src/components/DataSourceFormModal.jsx`: modal para cadastro de fonte de dados por tenant
- `src/components/ConfirmActionModal.jsx`: modal de confirmacao para acoes operacionais
- `src/components/MonitoredTableFormModal.jsx`: modal para cadastro de tabela monitorada
- `src/components/MonitoredColumnFormModal.jsx`: modal para cadastro de coluna monitorada
- `src/components/MfaCodeModal.jsx`: modal para confirmacao TOTP
- `src/pages/AccessPanel.jsx`: gestao de usuarios + reset MFA por admin
- `src/pages/ConfiguracaoPanel.jsx`: ativacao/desativacao MFA do proprio usuario
- `src/components/SetupAssistantModal.jsx`: assistente inicial com etapas pulaveis
- `src/components/TenantFormModal.jsx`: modal para cadastro de tenant
- `src/components/AppLlmConfigModal.jsx`: modal de configuracao da LLM padrao (superadmin)
- `src/components/TenantLlmConfigModal.jsx`: modal de configuracao da LLM do tenant
- `src/pages/ChatBiPanel.jsx`: pergunta NL + SQL assistido + resultado tabular
- `src/pages/IncidentPanel.jsx`: abertura + listagem de incidentes
- `src/pages/EventsPanel.jsx`: listagem de eventos estruturais
- `src/pages/OperationPanel.jsx`: painel de saude operacional
- `src/pages/AuditPanel.jsx`: trilha de chamadas MCP por tenant
- `src/pages/SqlSecurityPanel.jsx`: politica de seguranca SQL por tenant
- `src/api/mcpApi.js`: cliente HTTP MCP

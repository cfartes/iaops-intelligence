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
- `src/pages/ChatBiPanel.jsx`: pergunta NL + SQL assistido + resultado tabular
- `src/pages/IncidentPanel.jsx`: abertura + listagem de incidentes
- `src/pages/EventsPanel.jsx`: listagem de eventos estruturais
- `src/pages/OperationPanel.jsx`: painel de saude operacional
- `src/pages/AuditPanel.jsx`: trilha de chamadas MCP por tenant
- `src/pages/SqlSecurityPanel.jsx`: politica de seguranca SQL por tenant
- `src/api/mcpApi.js`: cliente HTTP MCP

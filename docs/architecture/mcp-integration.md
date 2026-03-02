# Integracao MCP (Model Context Protocol)

## Objetivo
Padronizar a integracao entre LLMs e recursos do IAOps com controle centralizado de seguranca, LGPD, auditoria e custo por tenant.

## Escopo
- IAOps como MCP Server: expoe tools internas do produto.
- IAOps como MCP Client: consome tools externas (ITSM, catalogo, mensageria, etc.).
- Politicas de autorizacao e conformidade no gateway MCP.

## Arquitetura alvo

### Componentes
1. MCP Gateway (backend Python)
- Entrada unica para chamadas MCP.
- Resolve identidade (`client_id`, `tenant_id`, `user_id`, `role`).
- Enforce de politicas (RBAC, LGPD, limites de plano).
- Telemetria e auditoria de cada chamada.

2. MCP Tool Registry
- Catalogo de tools disponiveis por tenant.
- Metadados: versao, schema de input/output, risco, timeout.

3. Policy Engine
- Avalia regras antes da execucao.
- Decide: `allow`, `deny`, `allow_with_masking`, `allow_with_limits`.

4. Tool Executors
- Adapters internos (inventario, SQL seguro, incidentes).
- Adapters externos (MCP client para servers de terceiros).

5. Audit and Usage Pipeline
- Log transacional da chamada.
- Metricas de latencia, status, tokens e custo.
- Relacao com faturamento quando usar LLM padrao do app.

### Fluxo de requisicao (server)
1. Recebe chamada MCP com contexto de autenticacao.
2. Valida se tenant esta ativo e adimplente.
3. Verifica role do usuario e escopo da tool.
4. Aplica politicas LGPD e seguranca SQL.
5. Executa tool com timeout e limites.
6. Aplica mascaramento no resultado (quando necessario).
7. Registra auditoria + telemetria + custo.
8. Retorna payload normalizado.

## Contratos iniciais de tools MCP

### 1) `inventory.list_tables`
- Objetivo: listar tabelas monitoradas do tenant.
- Role minima: `viewer`.
- Input:
  - `tenant_id` (obrigatorio)
  - `schema_name` (opcional)
- Output:
  - lista de objetos com `schema_name`, `table_name`, `is_active`

### 2) `inventory.list_columns`
- Objetivo: listar colunas da tabela monitorada.
- Role minima: `viewer`.
- Input:
  - `tenant_id`
  - `schema_name`
  - `table_name`
- Output:
  - lista com `column_name`, `data_type`, `classification`, `description_text`

### 3) `query.execute_safe_sql`
- Objetivo: executar SQL assistido com guardrails.
- Role minima: `admin` (ou `viewer` somente para consultas pre-aprovadas).
- Regras:
  - somente `SELECT`.
  - negar DDL/DML.
  - `LIMIT` maximo configuravel por plano.
  - whitelist de schemas/tabelas monitorados.
- Input:
  - `tenant_id`
  - `sql_text`
  - `explain` (bool, opcional)
- Output:
  - `rows`
  - `columns`
  - `applied_masks` (lista)
  - `execution_ms`

### 4) `incident.create`
- Objetivo: abrir incidente operacional.
- Role minima: `admin`.
- Input:
  - `tenant_id`
  - `title`
  - `severity`
  - `source_event_id` (opcional)
- Output:
  - `incident_id`
  - `status`
  - `sla_due_at`

### 5) `ops.get_health_summary`
- Objetivo: obter resumo de saude operacional.
- Role minima: `viewer`.
- Input:
  - `tenant_id`
  - `window_minutes` (opcional)
- Output:
  - `open_incidents`
  - `critical_events`
  - `channels_health`
  - `last_scan_at`

## Politicas obrigatorias no gateway
- Validacao de escopo tenant em 100% das calls.
- Deny by default para tool nao explicitamente habilitada.
- Bloqueio de operacoes quando cliente inadimplente alem da tolerancia.
- Aplicacao de mascaramento LGPD por coluna classificada como sensivel.
- Restricao de elevacao indevida: role efetiva nao pode exceder atribuicao em `tenant_user_role`.

## Modelo de dados adicional para MCP
- `mcp_server`: servidores MCP cadastrados no IAOps.
- `mcp_tool`: catalogo de tools do servidor.
- `tenant_mcp_tool_policy`: habilitacao/politica da tool por tenant.
- `mcp_client_connection`: conexoes para servers MCP externos por tenant.
- `mcp_call_log`: trilha detalhada de chamada (status, latencia, erro).
- `mcp_token_usage`: consumo de tokens/custo por chamada MCP.

## Regras de seguranca
- Credenciais de conexoes MCP externas por referencia de segredo criptografado.
- Sanitizacao de inputs antes de invocar executores.
- Timeout e circuit-breaker por tool.
- Rate limit por tenant e por usuario.
- Correlation ID obrigatorio para rastreabilidade ponta a ponta.

## Plano de implementacao (iterativo)

### Fase 1 - Fundacao
- Criar tabelas MCP e repositorios.
- Implementar MCP gateway com authn/authz e auditoria.
- Publicar 3 tools: `inventory.list_tables`, `inventory.list_columns`, `incident.create`.

### Fase 2 - SQL seguro
- Implementar `query.execute_safe_sql` com parser/guardrails.
- Integrar politicas LGPD por coluna.
- Medir latencia e volumetria.

### Fase 3 - MCP client externo
- Cadastrar conexoes por tenant.
- Habilitar chamadas outbound com allowlist.
- Observabilidade e retry controlado.

### Fase 4 - Billing e operacao
- Consolidar custo por token/tool no faturamento.
- Dashboard de uso MCP por tenant.
- Alertas de anomalia de custo/erro.

## Criterios de aceite (MVP MCP)
- Toda call MCP gera linha em `mcp_call_log`.
- Nenhuma call executa sem validacao de tenant e role.
- Tool SQL bloqueia qualquer comando fora de `SELECT`.
- Regras LGPD aplicadas e auditadas em respostas SQL.
- Latencia p95 de ferramentas internas dentro da meta definida em SLO.
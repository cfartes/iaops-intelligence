# MVP Backlog

## Epic A - Fundacao de conta e acesso
1. Cadastro self-service de cliente com confirmacao de e-mail.
2. Login com validacao de status da conta e senha segura.
3. Modelo de usuarios/papeis por tenant com anti-escalacao.

## Epic B - Tenant e monitoramento
1. Criar tenant e conectar data source.
2. Selecionar schemas/tabelas monitorados.
3. Job assincorno para snapshot de schema e deteccao de mudancas.
4. Registro de eventos com severidade.
5. Catálogo de conectores suportados (relacionais, NoSQL, warehouses, lakes, Power BI e Fabric).

## Epic C - Inventario e sugestoes
1. API de listagem de tabelas por tenant.
2. API de colunas por tabela.
3. Persistencia de catalogacao de colunas.
4. Sugestoes de classificacao via heuristica inicial.

## Epic D - Chat BI seguro
1. Sessao de chat por tenant com historico.
2. Pipeline NL->SQL com whitelist de comandos e limitador.
3. Aplicacao de regras LGPD em resultado.
4. Suporte LLM tenant + fallback padrao do app.
5. Metering de token para cobranca.

## Epic E - LGPD
1. CRUD de politica LGPD por tenant.
2. CRUD de regras dinamicas por coluna/tabela.
3. Registro de solicitacoes de titular.

## Epic F - Incidentes e operacao
1. Criacao de incidente via evento.
2. Fluxo de status (aberto, ack, resolvido, fechado).
3. SLA calculado por severidade.
4. Painel operacional resumido.

## Epic G - Billing
1. CRUD de planos (owner).
2. Assinatura do cliente e aplicacao de limites.
3. Geracao de fatura/parcelas.
4. Bloqueio de acesso por atraso > tolerancia.

## Epic H - Notificacoes
1. Config Telegram por tenant.
2. Config WhatsApp por tenant.
3. Indicadores de saude das integracoes.

## Epic I - MCP Gateway
1. Catalogo de MCP servers e tools internas.
2. Policy engine por tenant (enable, role minima, limites).
3. Auditoria e telemetria de chamadas MCP.
4. Tools iniciais: `source.list_catalog`, `inventory.list_tables`, `inventory.list_columns`, `query.execute_safe_sql`, `incident.create`.
5. Conexoes outbound para MCP servers externos por tenant.

## Ordem sugerida
1. Epic A
2. Epic B
3. Epic C
4. Epic E
5. Epic I
6. Epic D
7. Epic F
8. Epic G
9. Epic H
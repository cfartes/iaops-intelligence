# Visao e Escopo

## Objetivo
IAOps Governance e uma plataforma SaaS para:
- inventario de metadados por cliente/tenant;
- deteccao de mudancas estruturais (schema, tipo, regra de validacao);
- operacao com eventos, incidentes e SLA;
- consultas em linguagem natural com suporte LLM + SQL assistido;
- governanca de acesso e conformidade LGPD.

## Publico-alvo
- Empresas com operacao multi-tenant.
- Times de dados, BI, governanca, seguranca e operacoes.
- Gestores que precisam de controle de acesso, compliance e custo.

## Cobertura de fontes de dados (alvo)
- Bancos relacionais: SQL Server, PostgreSQL, MySQL, Oracle.
- NoSQL: MongoDB, Cassandra, DynamoDB.
- Data warehouses: Snowflake, BigQuery, Redshift.
- Data lakes/object storage: AWS S3, Azure Blob Storage, Google Cloud Storage.
- BI/semantic: Power BI e Microsoft Fabric.

## Escopo funcional
1. Cadastro e login self-service com confirmacao por e-mail.
2. Onboarding de tenant com conexao de origem e escopo monitorado.
3. Inventario de tabelas/colunas e sugestoes de classificacao.
4. Chat BI com politicas LGPD e regras de seguranca SQL.
5. Eventos, incidentes e painel operacional.
6. LGPD por tenant (politicas, mascaramento, solicitacoes de titular).
7. Gestao de usuarios/papeis com restricao de elevacao indevida.
8. Faturamento, parcelas e bloqueio por inadimplencia.
9. Canais Telegram e WhatsApp por tenant.
10. LLM por tenant com fallback para LLM padrao do app.

## Fora de escopo inicial (MVP)
- Workflow completo de atendimento juridico automatizado.
- Integracao com ERPs financeiros externos.
- Marketplace de conectores.

## Metas de negocio
- Reduzir risco operacional e de compliance.
- Aumentar visibilidade de mudancas em dados.
- Dar autonomia para consulta gerencial em linguagem natural.
- Controlar custos por cliente/tenant/uso de LLM.
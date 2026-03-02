# Modelo de Dominio (Logico)

## Entidades centrais
1. Client
- Representa a empresa contratante.
- Campos-chave: id, nome_fantasia, razao_social, cnpj, contatos, status.

2. Plan
- Define limites e preco (tenants, usuarios, valor mensal, tolerancia atraso).

3. Subscription
- Vincula client a plan em um periodo.

4. Tenant
- Ambiente operacional de um client.
- Possui configuracoes de monitoramento, LGPD, notificacoes e LLM.

5. User
- Identidade de acesso.

6. TenantUserRole
- Papel por tenant (`viewer`, `admin`, `owner`).

## Dados e monitoramento
7. DataSourceCatalog
- Catalogo de conectores suportados por categoria.
- Inclui bancos relacionais, NoSQL, warehouses, lakes, Power BI e Fabric.

8. DataSource
- Conexao de origem de dados por tenant.
- Referencia um item do catalogo (`source_type`/`code`).

9. MonitoredTable
- Tabela/schema monitorado por tenant.

10. MonitoredColumn
- Coluna monitorada e metadados de classificacao/governanca.

11. SchemaChangeEvent
- Mudanca detectada (tipo, severidade, payload).

12. Incident
- Ciclo operacional de incidente com SLA.

## LGPD e seguranca
13. LgpdPolicy
- Politica por tenant (DPO, retencao, observacoes).

14. LgpdRule
- Regra dinamica textual por coluna/tabela.

15. DataSubjectRequest
- Solicitacoes de titular (acesso, exclusao, etc).

16. AuditLog
- Trilha de operacoes criticas.

## Chat BI e LLM
17. LlmProvider
- Cadastro de modelo/provedor por client ou global.

18. TenantLlmConfig
- Config por tenant: propria LLM ou padrao do app.

19. ChatSession / ChatMessage
- Conversas de BI e metadados de execucao.

20. TokenUsage
- Consumo por tenant/client para cobranca da LLM padrao.

## Financeiro
21. Invoice
- Fatura agregada por periodo.

22. Installment
- Parcelas com vencimento, baixa e atraso.

## Relacoes principais
- Client 1:N Tenant
- Client 1:N User (identidade base) e User N:N Tenant via TenantUserRole
- Tenant 1:N DataSource
- Tenant 1:N MonitoredTable 1:N MonitoredColumn
- MonitoredTable 1:N SchemaChangeEvent
- Tenant 1:N Incident
- Tenant 1:1 LgpdPolicy e 1:N LgpdRule
- Client 1:N Invoice 1:N Installment
- Tenant 1:N TokenUsage

## Regras de isolamento
- Toda entidade operacional deve carregar `client_id` e/ou `tenant_id`.
- Consultas de app devem sempre filtrar escopo por tenant ativo do usuario.
- Owner global pode atravessar tenants, com trilha de auditoria obrigatoria.
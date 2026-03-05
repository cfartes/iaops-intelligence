# HUB Integration Contract (IAOps)

API HUB KEY IAops:  1be404e33e1204f554308bd9a741ae9e79cb14e977a1bd95

Este documento define o contrato de integracao entre o **HUB de Faturamento** e o **IAOps**.

Objetivo:
- HUB centraliza cobranca/baixa de todos os SaaS.
- IAOps fornece dados de faturamento por cliente.
- HUB atualiza no IAOps as datas financeiras e de liberacao.

## 1. Autenticacao

Recomendado para integracao servidor-servidor:
- Header: `X-IAOPS-HUB-KEY: <chave>`

A chave pode vir de 2 fontes (ordem de prioridade):
1. Variavel de ambiente: `IAOPS_HUB_API_KEY`
2. Configuracao salva no banco via tela de **Configuracao (Superadmin)**

Fallback:
- Sessao autenticada de superadmin.

Sem chave valida (ou sem superadmin), os endpoints retornam `403`.

## 2. Endpoints REST (Hub)

### 2.1 Listar clientes ativos para faturamento

- Metodo: `GET`
- Rota: `/api/hub/billing/clients`
- Header obrigatorio: `X-IAOPS-HUB-KEY`

Resposta de sucesso (`200`):

```json
{
  "status": "success",
  "tool": "billing_hub.list_clients",
  "correlation_id": "uuid",
  "data": {
    "app_name": "IAOps Governance",
    "clients": [
      {
        "client_id": 11,
        "cliente": "Santa Helena",
        "fantasy_name": "Santa Helena",
        "legal_name": "Santa Helena Ind Com Ltda",
        "cnpj": "00.000.000/0001-00",
        "access_email": "owner@empresa.com",
        "email_owner": "owner@empresa.com",
        "notification_email": "financeiro@empresa.com",
        "status": "active",
        "client_status": "active",
        "data_liberado": "2026-04-05",
        "data_pagamento": "2026-03-05",
        "data_ultimo_pagamento": "2026-03-05",
        "data_prox_vencimento": "2026-04-05",
        "data_proximo_vencimento": "2026-04-05",
        "plano": "Starter",
        "plan_code": "starter",
        "valor": 29900,
        "valor_cents": 29900,
        "due_base_day": 5
      }
    ]
  },
  "error": null
}
```

Campos principais para o HUB:
- `app_name`
- `cliente` ou `email_owner`
- `plano` / `plan_code`
- `valor_cents`
- `data_pagamento`
- `data_prox_vencimento`
- `data_liberado`
- `status`

### 2.2 Atualizar status financeiro/liberacao

- Metodo: `POST`
- Rota: `/api/hub/billing/release-date`
- Header obrigatorio: `X-IAOPS-HUB-KEY`

Body JSON:
- Identificador: `cliente` **ou** `email_owner`
- Datas:
  - `data_pagamento`
  - `data_prox_vencimento`
  - `data_liberado` (obrigatoria)

Exemplo recomendado:

```json
{
  "email_owner": "owner@empresa.com",
  "data_pagamento": "2026-03-05",
  "data_prox_vencimento": "2026-04-05",
  "data_liberado": "2026-04-05"
}
```

Compatibilidade (ainda aceito):
- Identificador: `client_id`, `cnpj`, `access_email`
- Datas: `data_ultimo_pagamento`, `data_proximo_vencimento`

Resposta de sucesso (`200`):

```json
{
  "status": "success",
  "tool": "billing_hub.update_release_date",
  "correlation_id": "uuid",
  "data": {
    "app_name": "IAOps Governance",
    "client": {
      "client_id": 11,
      "cliente": "Santa Helena",
      "email_owner": "owner@empresa.com",
      "cnpj": "00.000.000/0001-00",
      "data_pagamento": "2026-03-05",
      "data_prox_vencimento": "2026-04-05",
      "data_liberado": "2026-04-05",
      "status": "active"
    }
  },
  "error": null
}
```

Erros comuns:
- `400 invalid_input`: faltou identificador/datas invalidas.
- `400 hub_error`: cliente nao encontrado.
- `403 hub_auth_failed`: chave/sessao invalida.

## 3. Chamada MCP equivalente

Endpoint MCP:
- Metodo: `POST`
- Rota: `/api/mcp/call`

### 3.1 Tool: `billing_hub.list_clients`

```json
{
  "tool": "billing_hub.list_clients",
  "input": {}
}
```

### 3.2 Tool: `billing_hub.update_release_date`

```json
{
  "tool": "billing_hub.update_release_date",
  "input": {
    "email_owner": "owner@empresa.com",
    "data_pagamento": "2026-03-05",
    "data_prox_vencimento": "2026-04-05",
    "data_liberado": "2026-04-05"
  }
}
```

Observacao:
- Via MCP, a autorizacao segue o mesmo criterio (`X-IAOPS-HUB-KEY` ou superadmin).

## 4. Regra de bloqueio no IAOps

Um tenant fica operacional somente se:
- `tenant.status = 'active'`
- `client.status = 'active'`
- `client.data_liberado >= CURRENT_DATE`

Ou seja:
- Se `data_liberado` expirar, o cliente perde acesso automaticamente.
- Ao receber baixa no HUB, atualize `data_liberado` para reabilitar.

## 5. Exemplos cURL

### 5.1 Listar clientes

```bash
curl -X GET http://127.0.0.1:8000/api/hub/billing/clients \
  -H "X-IAOPS-HUB-KEY: SUA_CHAVE"
```

### 5.2 Atualizar datas financeiras/liberacao

```bash
curl -X POST http://127.0.0.1:8000/api/hub/billing/release-date \
  -H "Content-Type: application/json" \
  -H "X-IAOPS-HUB-KEY: SUA_CHAVE" \
  -d "{\"email_owner\":\"owner@empresa.com\",\"data_pagamento\":\"2026-03-05\",\"data_prox_vencimento\":\"2026-04-05\",\"data_liberado\":\"2026-04-05\"}"
```

### 5.3 MCP listar

```bash
curl -X POST http://127.0.0.1:8000/api/mcp/call \
  -H "Content-Type: application/json" \
  -H "X-IAOPS-HUB-KEY: SUA_CHAVE" \
  -d "{\"tool\":\"billing_hub.list_clients\",\"input\":{}}"
```

## 6. Configuracao de chave no IAOps (Superadmin)

Tela: **Configuracao**
- Secao: **Integracao HUB de Faturamento (Superadmin)**
- Acoes:
  - Salvar HUB API Key
  - Gerar nova chave
  - Atualizar chave

Persistencia:
- Tabela: `iaops_gov.app_hub_integration_config`
- Campo criptografado: `hub_api_key_enc`

## 7. Padrao recomendado para outros SaaS

Para padronizar todos os apps no HUB, recomenda-se expor as mesmas operacoes:

1. `GET /api/hub/billing/clients`
- Retorno minimo: `app_name`, `cliente`/`email_owner`, `plano`, `valor_cents`, `data_pagamento`, `data_prox_vencimento`, `data_liberado`, `status`

2. `POST /api/hub/billing/release-date`
- Entrada minima: `cliente` ou `email_owner` + `data_pagamento` + `data_prox_vencimento` + `data_liberado`

3. Regra de acesso no SaaS
- Liberar enquanto `data_liberado >= hoje`

Com isso, o HUB usa um unico conector para todos os produtos.

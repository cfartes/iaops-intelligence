# Requisitos Funcionais e Regras

## Hierarquia e acesso
- Cliente: entidade comercial contratante.
- Tenant: ambiente operacional de um cliente.
- Usuario: pessoa com acesso.
- Um cliente possui multiplos tenants conforme plano.
- Papeis por tenant: `viewer`, `admin`, `owner`.
- `owner` global possui administracao ampliada.

## RF-01 Cadastro e autenticacao
- Self-signup de novo cliente sem superadmin.
- Campos obrigatorios:
  - nome_fantasia
  - razao_social
  - cnpj
  - endereco
  - telefone_contato
  - email_contato
  - email_acesso
  - email_notificacao
  - senha
  - plano_escolhido
- Confirmacao de e-mail obrigatoria antes do primeiro acesso.
- Senha com hash forte.
- MFA opcional por usuario via TOTP, com ativacao voluntaria no perfil.
- Admin/owner do cliente pode resetar MFA de usuarios do tenant em caso de perda/troca de celular.

## RF-02 Onboarding do tenant`r`n- Assistente inicial de configuracao no primeiro acesso com etapas pulaveis (configurar agora ou depois).
- Cadastro de fonte de dados por tenant.
- Escolha de schemas/tabelas monitorados.
- Configuracao de canais de alerta.
- Configuracao de agendamento de varredura.
- Catálogo de fontes suportadas deve incluir:
  - relacionais: SQL Server, PostgreSQL, MySQL, Oracle
  - NoSQL: MongoDB, Cassandra, DynamoDB
  - warehouses: Snowflake, BigQuery, Redshift
  - lakes/object storage: AWS S3, Azure Blob, Google Cloud Storage
  - BI/semantic: Power BI e Microsoft Fabric

## RF-03 Inventario e sugestoes
- Lista de tabelas por tenant.
- Lista de colunas ao selecionar tabela.
- Catalogacao de colunas monitoradas.
- Sugestoes de classificacao e descricao para governanca.

## RF-04 Chat BI
- Entrada por linguagem natural.
- Saida priorizada em texto natural.
- Opcao de detalhes tecnicos sob demanda.
- Geracao SQL assistida com guardrails.
- Aplicacao de regras LGPD no resultado.
- Suporte a LLM do tenant ou LLM padrao do app.`r`n- Quando tenant opta por usar LLM padrao do app, consumo de tokens deve ser contabilizado em todas as funcionalidades.

## RF-05 Eventos e incidentes
- Registro de eventos de monitoramento.
- Abertura de incidente a partir de evento.
- Fluxo de incidente: aberto, ack, resolvido, fechado.
- SLA por severidade e tenant.
- Painel de saude operacional.

## RF-06 LGPD
- Politica LGPD por tenant (DPO, contatos, retencao, observacoes legais).
- Regras dinamicas textuais de mascaramento/anonimizacao.
- Controle por coluna para dados sensiveis.
- Registro de solicitacoes de titular de dados.

## RF-07 Governanca de usuarios
- Gestao de usuarios e papeis por tenant.
- Ativacao/desativacao de usuario.
- Restricoes anti-escalacao de privilegio.

## RF-08 Faturamento e parcelas (owner)`r`n- Quando atingir limite de tenants ativos do plano, cliente pode desabilitar tenant para liberar vaga e criar outro.
- Cadastro de planos com limites e preco.
- Assinatura por cliente.
- Geracao de parcelas com vencimento.
- Baixa de pagamento e status de atraso.
- Bloqueio de acesso por inadimplencia acima da tolerancia.

## RF-09 Notificacoes`r`n- Em canais WhatsApp/Telegram, usuario pode listar tenants permitidos e selecionar tenant ativo da conversa.
- Canais por tenant: Telegram e WhatsApp.
- Indicadores de status de configuracao/servico.

## RF-10 Preferencias
- Idioma e preferencia visual por usuario+tenant.

## NFR (nao funcionais)
- Multi-tenant com isolamento logico por tenant_id.
- Auditoria de operacoes criticas.
- Segredos com criptografia simetrica em repouso.
- Disponibilidade com workers assincornos (Celery).
- Observabilidade minima: logs estruturados e metricas basicas.




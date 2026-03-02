import { useEffect, useMemo, useState } from "react";
import {
  channelGetActiveTenant,
  channelListUserTenants,
  channelSelectTenant,
  channelWebhookTelegram,
  getAuthContext,
  channelWebhookWhatsapp,
  enqueueHousekeepingJob,
  getObservabilityMetrics,
  getOperationHealth,
  listAsyncJobs,
  retryAsyncJob,
} from "../api/mcpApi";
import { tUi } from "../i18n/uiText";

export default function OperationPanel({ onSystemMessage }) {
  const authContext = getAuthContext();
  const jobsViewStateKey = useMemo(() => {
    const clientId = authContext?.client_id || 0;
    const tenantId = authContext?.tenant_id || 0;
    const userId = authContext?.user_id || 0;
    return `iaops_jobs_view_v1:${clientId}:${tenantId}:${userId}`;
  }, [authContext?.client_id, authContext?.tenant_id, authContext?.user_id]);
  const [health, setHealth] = useState(null);
  const [observability, setObservability] = useState(null);
  const [channelType, setChannelType] = useState("telegram");
  const [externalUserKey, setExternalUserKey] = useState("tg-owner-demo");
  const [conversationKey, setConversationKey] = useState("chat-owner-demo");
  const [messageText, setMessageText] = useState("tenant list");
  const [webhookResponse, setWebhookResponse] = useState(null);
  const [isSending, setIsSending] = useState(false);
  const [tenantOptions, setTenantOptions] = useState([]);
  const [selectedTenantId, setSelectedTenantId] = useState("");
  const [activeTenantLabel, setActiveTenantLabel] = useState("");
  const [isLoadingTenants, setIsLoadingTenants] = useState(false);
  const [isLoadingActive, setIsLoadingActive] = useState(false);
  const [isSelectingTenant, setIsSelectingTenant] = useState(false);
  const [jobs, setJobs] = useState([]);
  const [isLoadingJobs, setIsLoadingJobs] = useState(false);
  const [retryingJobId, setRetryingJobId] = useState(null);
  const [jobStatusFilter, setJobStatusFilter] = useState("");
  const [jobKindFilter, setJobKindFilter] = useState("");
  const [autoRefreshJobs, setAutoRefreshJobs] = useState(true);
  const [jobsPage, setJobsPage] = useState(0);
  const [jobsPageSize, setJobsPageSize] = useState(20);
  const [jobSortBy, setJobSortBy] = useState("id");
  const [jobSortDir, setJobSortDir] = useState("desc");
  const [jobsViewLoaded, setJobsViewLoaded] = useState(false);

  const baseChannelPayload = () => ({
    channel_type: channelType,
    external_user_key: externalUserKey.trim(),
    conversation_key: conversationKey.trim(),
  });

  const ensureChannelKeys = () => {
    if (!externalUserKey.trim() || !conversationKey.trim()) {
      onSystemMessage(
        "warning",
        tUi("op.required.title", "Campos obrigatorios"),
        tUi("op.required.message", "Informe external_user_key e conversation_key.")
      );
      return false;
    }
    return true;
  };

  const loadHealth = async () => {
    try {
      const data = await getOperationHealth(60);
      setHealth(data);
    } catch (error) {
      onSystemMessage("error", tUi("op.fail.health", "Erro ao carregar saude operacional"), error.message);
    }
  };

  const loadObservability = async () => {
    try {
      const data = await getObservabilityMetrics();
      setObservability(data);
    } catch (error) {
      onSystemMessage("error", "Erro ao carregar observabilidade", error.message);
    }
  };

  const loadJobs = async () => {
    setIsLoadingJobs(true);
    try {
      const data = await listAsyncJobs(jobsPageSize, jobsPage * jobsPageSize);
      setJobs(data.jobs || []);
    } catch (error) {
      onSystemMessage("error", "Falha ao carregar jobs", error.message);
    } finally {
      setIsLoadingJobs(false);
    }
  };

  const visibleJobs = useMemo(() => {
    const filtered = jobs.filter((job) => {
      const status = String(job.status || "").toLowerCase();
      const kind = String(job.job_kind || "").toLowerCase();
      const statusOk = !jobStatusFilter || status === jobStatusFilter;
      const kindOk = !jobKindFilter || kind === jobKindFilter;
      return statusOk && kindOk;
    });
    const sorted = [...filtered].sort((a, b) => {
      let av = a?.[jobSortBy];
      let bv = b?.[jobSortBy];
      if (jobSortBy === "created_at" || jobSortBy === "finished_at") {
        av = av ? new Date(av).getTime() : 0;
        bv = bv ? new Date(bv).getTime() : 0;
      }
      if (typeof av === "string") av = av.toLowerCase();
      if (typeof bv === "string") bv = bv.toLowerCase();
      if (av === bv) return 0;
      const base = av > bv ? 1 : -1;
      return jobSortDir === "asc" ? base : -base;
    });
    return sorted;
  }, [jobs, jobStatusFilter, jobKindFilter, jobSortBy, jobSortDir]);

  const jobKinds = useMemo(() => {
    return Array.from(new Set(jobs.map((job) => String(job.job_kind || "")).filter(Boolean))).sort();
  }, [jobs]);

  useEffect(() => {
    loadHealth();
    loadObservability();
  }, []);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(jobsViewStateKey);
      if (!raw) {
        setJobsViewLoaded(true);
        return;
      }
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") {
        setJobsViewLoaded(true);
        return;
      }
      if (typeof parsed.jobStatusFilter === "string") setJobStatusFilter(parsed.jobStatusFilter);
      if (typeof parsed.jobKindFilter === "string") setJobKindFilter(parsed.jobKindFilter);
      if (typeof parsed.autoRefreshJobs === "boolean") setAutoRefreshJobs(parsed.autoRefreshJobs);
      if (typeof parsed.jobsPage === "number") setJobsPage(Math.max(0, Math.floor(parsed.jobsPage)));
      if ([10, 20, 50].includes(Number(parsed.jobsPageSize))) setJobsPageSize(Number(parsed.jobsPageSize));
      if (["id", "job_kind", "status", "created_at", "finished_at"].includes(String(parsed.jobSortBy || ""))) {
        setJobSortBy(String(parsed.jobSortBy));
      }
      if (["asc", "desc"].includes(String(parsed.jobSortDir || ""))) setJobSortDir(String(parsed.jobSortDir));
    } catch (_) {
      // ignorar estado invalido no localStorage
    } finally {
      setJobsViewLoaded(true);
    }
  }, [jobsViewStateKey]);

  useEffect(() => {
    if (!jobsViewLoaded) return;
    try {
      window.localStorage.setItem(
        jobsViewStateKey,
        JSON.stringify(
          {
            jobStatusFilter,
            jobKindFilter,
            autoRefreshJobs,
            jobsPage,
            jobsPageSize,
            jobSortBy,
            jobSortDir,
          },
          null,
          0,
        ),
      );
    } catch (_) {
      // localStorage pode estar indisponivel
    }
  }, [
    jobsViewStateKey,
    jobsViewLoaded,
    jobStatusFilter,
    jobKindFilter,
    autoRefreshJobs,
    jobsPage,
    jobsPageSize,
    jobSortBy,
    jobSortDir,
  ]);

  useEffect(() => {
    loadJobs();
  }, [jobsPage, jobsPageSize]);

  useEffect(() => {
    if (!autoRefreshJobs) return undefined;
    const timer = window.setInterval(() => {
      loadJobs();
    }, 10000);
    return () => window.clearInterval(timer);
  }, [autoRefreshJobs, jobsPage, jobsPageSize]);

  useEffect(() => {
    if (channelType === "telegram") {
      setExternalUserKey("tg-owner-demo");
      setConversationKey("chat-owner-demo");
      return;
    }
    setExternalUserKey("wa-owner-demo");
    setConversationKey("wa-owner-demo");
    setTenantOptions([]);
    setSelectedTenantId("");
    setActiveTenantLabel("");
  }, [channelType]);

  const loadChannelTenants = async () => {
    if (!ensureChannelKeys()) return;
    setIsLoadingTenants(true);
    try {
      const data = await channelListUserTenants(baseChannelPayload());
      const tenants = data.tenants || [];
      setTenantOptions(tenants);
      setSelectedTenantId((prev) => {
        if (prev && tenants.some((item) => String(item.tenant_id) === String(prev))) {
          return prev;
        }
        return tenants[0] ? String(tenants[0].tenant_id) : "";
      });
      onSystemMessage(
        "success",
        tUi("op.tenant.loaded.title", "Tenants carregados"),
        tUi("op.tenant.loaded.message", "{count} tenant(s) disponivel(is) para este usuario/canal.", {
          count: tenants.length,
        })
      );
    } catch (error) {
      onSystemMessage("error", tUi("op.tenant.fail.title", "Falha na gestao de tenant"), error.message);
    } finally {
      setIsLoadingTenants(false);
    }
  };

  const loadActiveTenant = async () => {
    if (!ensureChannelKeys()) return;
    setIsLoadingActive(true);
    try {
      const data = await channelGetActiveTenant(baseChannelPayload());
      const activeTenantId = data.active_tenant_id;
      const tenants = data.tenants || tenantOptions;
      if (tenants.length > 0 && tenantOptions.length === 0) {
        setTenantOptions(tenants);
      }
      if (activeTenantId == null) {
        setActiveTenantLabel(tUi("op.tenant.active.none", "Nenhum tenant ativo na conversa."));
        return;
      }
      const selected = tenants.find((item) => String(item.tenant_id) === String(activeTenantId));
      const label = selected
        ? `${selected.tenant_id} - ${selected.name} (${selected.status}, ${selected.role})`
        : tUi("op.tenant.active.onlyId", "Tenant ativo: {tenant_id}", { tenant_id: activeTenantId });
      setActiveTenantLabel(label);
      setSelectedTenantId(String(activeTenantId));
    } catch (error) {
      onSystemMessage("error", tUi("op.tenant.fail.title", "Falha na gestao de tenant"), error.message);
    } finally {
      setIsLoadingActive(false);
    }
  };

  const selectActiveTenant = async () => {
    if (!ensureChannelKeys()) return;
    if (!selectedTenantId) {
      onSystemMessage(
        "warning",
        tUi("op.required.title", "Campos obrigatorios"),
        tUi("op.tenant.select.required", "Selecione um tenant para ativar no canal.")
      );
      return;
    }
    setIsSelectingTenant(true);
    try {
      await channelSelectTenant({
        ...baseChannelPayload(),
        tenant_id: Number(selectedTenantId),
      });
      await loadActiveTenant();
      onSystemMessage(
        "success",
        tUi("op.tenant.select.ok.title", "Tenant ativo atualizado"),
        tUi("op.tenant.select.ok.message", "Tenant da conversa atualizado com sucesso.")
      );
    } catch (error) {
      onSystemMessage("error", tUi("op.tenant.fail.title", "Falha na gestao de tenant"), error.message);
    } finally {
      setIsSelectingTenant(false);
    }
  };

  const sendChannelMessage = async () => {
    if (!ensureChannelKeys()) return;
    setIsSending(true);
    setWebhookResponse(null);
    try {
      const payload = {
        external_user_key: externalUserKey.trim(),
        conversation_key: conversationKey.trim(),
        text: messageText.trim(),
      };
      const data =
        channelType === "telegram"
          ? await channelWebhookTelegram(payload)
          : await channelWebhookWhatsapp(payload);
      setWebhookResponse(data);
      onSystemMessage("success", tUi("op.webhook.ok.title", "Webhook processado"), tUi("op.webhook.ok.message", "Mensagem processada com sucesso no canal."));
    } catch (error) {
      onSystemMessage("error", tUi("op.webhook.fail.title", "Erro no webhook"), error.message);
    } finally {
      setIsSending(false);
    }
  };

  const retryJob = async (jobId) => {
    setRetryingJobId(jobId);
    try {
      await retryAsyncJob({ job_id: Number(jobId) });
      onSystemMessage("success", "Job reenfileirado", `Reprocessamento do job ${jobId} iniciado.`);
      await loadJobs();
    } catch (error) {
      onSystemMessage("error", "Falha ao reprocessar job", error.message);
    } finally {
      setRetryingJobId(null);
    }
  };

  const runHousekeeping = async () => {
    try {
      await enqueueHousekeepingJob({ retention_days: 90 });
      onSystemMessage("success", "Housekeeping", "Job de limpeza enfileirado.");
      await loadJobs();
    } catch (error) {
      onSystemMessage("error", "Falha housekeeping", error.message);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>{tUi("op.header.title", "Operacao")}</h2>
        <p>{tUi("op.header.subtitle", "Painel de saude operacional e canais de notificacao.")}</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-secondary" onClick={loadHealth}>
          {tUi("op.refresh", "Atualizar Saude")}
        </button>
        <button type="button" className="btn btn-secondary" onClick={loadObservability}>
          Atualizar Observabilidade
        </button>
        <button type="button" className="btn btn-secondary" onClick={loadJobs}>
          Atualizar Jobs
        </button>
        <button type="button" className="btn btn-secondary" onClick={runHousekeeping}>
          Rodar Housekeeping
        </button>
        <button
          type="button"
          className="btn btn-secondary"
          onClick={() => setAutoRefreshJobs((prev) => !prev)}
        >
          {autoRefreshJobs ? "Auto-refresh jobs: ON" : "Auto-refresh jobs: OFF"}
        </button>
      </div>

      {!health && <p className="empty-state">{tUi("op.empty", "Sem dados de saude.")}</p>}

      {health && (
        <div className="metric-grid">
          <article className="metric-card">
            <h4>{tUi("op.metric.openIncidents", "Incidentes abertos")}</h4>
            <strong>{health.open_incidents}</strong>
          </article>
          <article className="metric-card">
            <h4>{tUi("op.metric.criticalEvents", "Eventos criticos (janela)")}</h4>
            <strong>{health.critical_events}</strong>
          </article>
          <article className="metric-card">
            <h4>{tUi("op.metric.lastScan", "Ultima varredura")}</h4>
            <strong>{health.last_scan_at || "n/a"}</strong>
          </article>
          <article className="metric-card">
            <h4>{tUi("op.metric.channels", "Canais")}</h4>
            <div className="chip-row">
              {Object.entries(health.channels_health || {}).map(([name, status]) => (
                <span key={name} className="chip">{name}: {status}</span>
              ))}
            </div>
          </article>
        </div>
      )}

      {observability && (
        <section className="catalog-block">
          <h3>Observabilidade</h3>
          {(Number(observability.jobs_dead_letter || 0) > 0 || Number(observability.lgpd_blocked_24h || 0) > 50) ? (
            <div className="chip-row">
              {Number(observability.jobs_dead_letter || 0) > 0 ? (
                <span className="chip">Alerta: ha jobs em dead-letter.</span>
              ) : null}
              {Number(observability.lgpd_blocked_24h || 0) > 50 ? (
                <span className="chip">Alerta: bloqueios LGPD acima do padrao em 24h.</span>
              ) : null}
            </div>
          ) : null}
          <div className="metric-grid">
            <article className="metric-card">
              <h4>Jobs retrying</h4>
              <strong>{observability.jobs_retrying ?? 0}</strong>
            </article>
            <article className="metric-card">
              <h4>Jobs dead-letter</h4>
              <strong>{observability.jobs_dead_letter ?? 0}</strong>
            </article>
            <article className="metric-card">
              <h4>Bloqueios LGPD (24h)</h4>
              <strong>{observability.lgpd_blocked_24h ?? 0}</strong>
            </article>
            <article className="metric-card">
              <h4>LLM tokens (24h)</h4>
              <strong>{observability.llm_tokens_24h ?? 0}</strong>
            </article>
            <article className="metric-card">
              <h4>Custo LLM (24h)</h4>
              <strong>{((observability.llm_amount_cents_24h ?? 0) / 100).toFixed(2)}</strong>
            </article>
          </div>
        </section>
      )}

      <section className="catalog-block channel-tester">
        <h3>{tUi("op.tester.title", "Tester de Canal (Telegram/WhatsApp)")}</h3>
        <p className="muted">
          {tUi("op.tester.subtitle", "Simula entrada de webhook com comandos e linguagem natural.")}
        </p>

        <div className="inline-form">
          <select value={channelType} onChange={(event) => setChannelType(event.target.value)}>
            <option value="telegram">Telegram</option>
            <option value="whatsapp">WhatsApp</option>
          </select>
          <input
            value={externalUserKey}
            onChange={(event) => setExternalUserKey(event.target.value)}
            placeholder="external_user_key"
          />
          <input
            value={conversationKey}
            onChange={(event) => setConversationKey(event.target.value)}
            placeholder="conversation_key"
          />
        </div>

        <div className="inline-form">
          <input
            value={messageText}
            onChange={(event) => setMessageText(event.target.value)}
            placeholder={tUi("op.tester.message.placeholder", "Mensagem / comando")}
          />
          <button type="button" className="btn btn-primary" onClick={sendChannelMessage} disabled={isSending}>
            {isSending ? tUi("op.tester.sending", "Enviando...") : tUi("op.tester.send", "Enviar para Webhook")}
          </button>
        </div>

        {webhookResponse && (
          <article className="metric-card webhook-output">
            <h4>{tUi("op.tester.reply", "Resposta do Bot")}</h4>
            <pre>{webhookResponse.reply_text || tUi("op.tester.noReply", "Sem resposta textual")}</pre>
          </article>
        )}
      </section>

      <section className="catalog-block channel-tester">
        <h3>{tUi("op.tenant.title", "Gestao de Tenant no Canal")}</h3>
        <p className="muted">
          {tUi("op.tenant.subtitle", "Liste tenants disponiveis e defina o tenant ativo da conversa sem comandos tecnicos.")}
        </p>

        <div className="inline-form">
          <button type="button" className="btn btn-secondary" onClick={loadChannelTenants} disabled={isLoadingTenants}>
            {isLoadingTenants ? tUi("op.tenant.loading", "Carregando...") : tUi("op.tenant.list", "Listar Tenants")}
          </button>
          <button type="button" className="btn btn-secondary" onClick={loadActiveTenant} disabled={isLoadingActive}>
            {isLoadingActive ? tUi("op.tenant.loading", "Carregando...") : tUi("op.tenant.active.get", "Ver Tenant Ativo")}
          </button>
        </div>

        <div className="inline-form">
          <select value={selectedTenantId} onChange={(event) => setSelectedTenantId(event.target.value)}>
            <option value="">{tUi("op.tenant.select.placeholder", "Selecione um tenant")}</option>
            {tenantOptions.map((item) => (
              <option key={item.tenant_id} value={String(item.tenant_id)}>
                {`${item.tenant_id} - ${item.name} (${item.status}, ${item.role})`}
              </option>
            ))}
          </select>
          <button type="button" className="btn btn-primary" onClick={selectActiveTenant} disabled={isSelectingTenant || !selectedTenantId}>
            {isSelectingTenant ? tUi("op.tenant.select.saving", "Atualizando...") : tUi("op.tenant.select.set", "Definir Tenant Ativo")}
          </button>
        </div>

        <article className="metric-card webhook-output">
          <h4>{tUi("op.tenant.active.title", "Tenant ativo da conversa")}</h4>
          <pre>{activeTenantLabel || tUi("op.tenant.active.none", "Nenhum tenant ativo na conversa.")}</pre>
        </article>
      </section>

      <section className="catalog-block">
        <h3>Jobs Assincronos</h3>
        <p className="muted">Fila de processamento por tenant com status e retry de falhas.</p>
        <div className="inline-form">
          <label>
            Itens por pagina
            <select
              value={jobsPageSize}
              onChange={(event) => {
                setJobsPage(0);
                setJobsPageSize(Number(event.target.value));
              }}
            >
              <option value={10}>10</option>
              <option value={20}>20</option>
              <option value={50}>50</option>
            </select>
          </label>
          <button
            type="button"
            className="btn btn-secondary"
            onClick={() => setJobsPage(0)}
            disabled={isLoadingJobs || jobsPage === 0}
          >
            Primeira
          </button>
          <button
            type="button"
            className="btn btn-secondary"
            onClick={() => setJobsPage((prev) => Math.max(0, prev - 1))}
            disabled={isLoadingJobs || jobsPage === 0}
          >
            Anterior
          </button>
          <button
            type="button"
            className="btn btn-secondary"
            onClick={() => setJobsPage((prev) => prev + 1)}
            disabled={isLoadingJobs || jobs.length < jobsPageSize}
          >
            Proxima
          </button>
          <span className="chip">Pagina {jobsPage + 1}</span>
        </div>
        <div className="inline-form">
          <select value={jobStatusFilter} onChange={(event) => setJobStatusFilter(event.target.value)}>
            <option value="">Todos status</option>
            <option value="queued">queued</option>
            <option value="running">running</option>
            <option value="retrying">retrying</option>
            <option value="done">done</option>
            <option value="failed">failed</option>
            <option value="dead_letter">dead_letter</option>
          </select>
          <select value={jobKindFilter} onChange={(event) => setJobKindFilter(event.target.value)}>
            <option value="">Todos tipos</option>
            {jobKinds.map((kind) => (
              <option key={kind} value={kind}>
                {kind}
              </option>
            ))}
          </select>
          <button
            type="button"
            className="btn btn-secondary"
            onClick={() => {
              setJobStatusFilter("");
              setJobKindFilter("");
            }}
          >
            Limpar filtros
          </button>
          <select value={jobSortBy} onChange={(event) => setJobSortBy(event.target.value)}>
            <option value="id">Ordenar por ID</option>
            <option value="job_kind">Ordenar por tipo</option>
            <option value="status">Ordenar por status</option>
            <option value="created_at">Ordenar por criado em</option>
            <option value="finished_at">Ordenar por finalizado em</option>
          </select>
          <select value={jobSortDir} onChange={(event) => setJobSortDir(event.target.value)}>
            <option value="desc">DESC</option>
            <option value="asc">ASC</option>
          </select>
        </div>
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Tipo</th>
                <th>Status</th>
                <th>Criado em</th>
                <th>Fim</th>
                <th>Acoes</th>
              </tr>
            </thead>
            <tbody>
              {isLoadingJobs ? (
                <tr>
                  <td colSpan={6}>Carregando jobs...</td>
                </tr>
              ) : visibleJobs.length === 0 ? (
                <tr>
                  <td colSpan={6}>Nenhum job encontrado.</td>
                </tr>
              ) : (
                visibleJobs.map((job) => {
                  const canRetry = ["failed", "dead_letter"].includes(String(job.status || "").toLowerCase());
                  return (
                    <tr key={job.id}>
                      <td>{job.id}</td>
                      <td>{job.job_kind}</td>
                      <td>{job.status}</td>
                      <td>{job.created_at || "-"}</td>
                      <td>{job.finished_at || "-"}</td>
                      <td>
                        {canRetry ? (
                          <button
                            type="button"
                            className="btn btn-small btn-secondary"
                            onClick={() => retryJob(job.id)}
                            disabled={retryingJobId === job.id}
                          >
                            {retryingJobId === job.id ? "Reenfileirando..." : "Reprocessar"}
                          </button>
                        ) : (
                          "-"
                        )}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  );
}

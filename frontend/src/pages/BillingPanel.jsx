import { useEffect, useMemo, useState } from "react";
import {
  deleteBillingPlan,
  downloadBillingLlmUsageCsv,
  enqueueIngestionJob,
  getAuthContext,
  enqueueRagRebuildJob,
  getBillingLlmUsage,
  getBillingSubscription,
  getObservabilityMetrics,
  listClientTenants,
  listAsyncJobs,
  listBillingPlans,
  upsertBillingPlan,
  upsertBillingSubscription,
} from "../api/mcpApi";

export default function BillingPanel({ onSystemMessage }) {
  const auth = getAuthContext();
  const authRole = String(auth?.role || "").toLowerCase();
  const isGlobalSuperadmin = Boolean(auth?.is_superadmin) && Number(auth?.tenant_id || 0) <= 0;
  const canSelectUsageTenant = !isGlobalSuperadmin && ["owner", "admin", "superadmin"].includes(authRole);
  const [plans, setPlans] = useState([]);
  const [subscription, setSubscription] = useState({});
  const [jobs, setJobs] = useState([]);
  const [metrics, setMetrics] = useState({});
  const [showModal, setShowModal] = useState(false);
  const [planCode, setPlanCode] = useState("starter");
  const [toleranceDays, setToleranceDays] = useState("5");
  const [llmUsageDays, setLlmUsageDays] = useState("30");
  const [llmUsage, setLlmUsage] = useState({ summary: {}, by_feature: [], recent: [] });
  const [tenantOptions, setTenantOptions] = useState([]);
  const [usageTenantId, setUsageTenantId] = useState("");
  const [planModalOpen, setPlanModalOpen] = useState(false);
  const [planStatusFilter, setPlanStatusFilter] = useState("all");
  const [planSortBy, setPlanSortBy] = useState("code");
  const [planSortDir, setPlanSortDir] = useState("asc");
  const [planDraft, setPlanDraft] = useState({
    id: "",
    code: "",
    name: "",
    max_tenants: 1,
    max_users: 1,
    max_data_sources_per_client: 10,
    max_data_sources_per_tenant: 5,
    monthly_price_cents: 0,
    is_active: true,
  });

  const loadAll = async () => {
    try {
      const [p, s, j, m, u] = await Promise.all([
        listBillingPlans(),
        getBillingSubscription(),
        listAsyncJobs(20),
        getObservabilityMetrics(),
        getBillingLlmUsage(Number(llmUsageDays) || 30, usageTenantId || undefined),
      ]);
      let tenants = [];
      if (!isGlobalSuperadmin) {
        try {
          const t = await listClientTenants();
          tenants = t.tenants || [];
        } catch (_) {
          tenants = [];
        }
      }
      setPlans(p.plans || []);
      setSubscription(s.subscription || {});
      setJobs(j.jobs || []);
      setMetrics(m || {});
      setLlmUsage(u || { summary: {}, by_feature: [], recent: [] });
      setTenantOptions(tenants);
      if (!usageTenantId && tenants.length > 0) {
        setUsageTenantId(String(tenants[0].id));
      }
    } catch (error) {
      onSystemMessage("error", "Falha faturamento", error.message);
    }
  };

  useEffect(() => {
    loadAll();
  }, [llmUsageDays, isGlobalSuperadmin]);

  const saveSubscription = async () => {
    try {
      await upsertBillingSubscription({ plan_code: planCode, tolerance_days: Number(toleranceDays) });
      setShowModal(false);
      onSystemMessage("success", "Faturamento", "Assinatura atualizada.");
      await loadAll();
    } catch (error) {
      onSystemMessage("error", "Falha faturamento", error.message);
    }
  };

  const openNewPlanModal = () => {
    setPlanDraft({
      id: "",
      code: "",
      name: "",
      max_tenants: 1,
      max_users: 1,
      max_data_sources_per_client: 10,
      max_data_sources_per_tenant: 5,
      monthly_price_cents: 0,
      is_active: true,
    });
    setPlanModalOpen(true);
  };

  const openEditPlanModal = (item) => {
    setPlanDraft({
      id: item?.id || "",
      code: item?.code || "",
      name: item?.name || "",
      max_tenants: Number(item?.max_tenants || 1),
      max_users: Number(item?.max_users || 1),
      max_data_sources_per_client: Number(item?.max_data_sources_per_client || 10),
      max_data_sources_per_tenant: Number(item?.max_data_sources_per_tenant || 5),
      monthly_price_cents: Number(item?.monthly_price_cents || 0),
      is_active: Boolean(item?.is_active),
    });
    setPlanModalOpen(true);
  };

  const savePlan = async () => {
    try {
      await upsertBillingPlan({
        id: planDraft.id || undefined,
        code: String(planDraft.code || "").trim().toLowerCase(),
        name: String(planDraft.name || "").trim(),
        max_tenants: Number(planDraft.max_tenants || 1),
        max_users: Number(planDraft.max_users || 1),
        max_data_sources_per_client: Number(planDraft.max_data_sources_per_client || 10),
        max_data_sources_per_tenant: Number(planDraft.max_data_sources_per_tenant || 5),
        monthly_price_cents: Number(planDraft.monthly_price_cents || 0),
        is_active: Boolean(planDraft.is_active),
      });
      setPlanModalOpen(false);
      onSystemMessage("success", "Planos", "Plano salvo com sucesso.");
      await loadAll();
    } catch (error) {
      onSystemMessage("error", "Falha ao salvar plano", error.message);
    }
  };

  const removePlan = async (item) => {
    const confirmed = window.confirm(`Excluir/inativar o plano ${item?.name || item?.code}?`);
    if (!confirmed) return;
    try {
      const data = await deleteBillingPlan({ id: item?.id });
      if (data?.inactivated) {
        onSystemMessage("warning", "Plano inativado", "Plano em uso por assinatura foi apenas inativado.");
      } else {
        onSystemMessage("success", "Plano removido", "Plano removido com sucesso.");
      }
      await loadAll();
    } catch (error) {
      onSystemMessage("error", "Falha ao excluir plano", error.message);
    }
  };

  const displayedPlans = useMemo(() => {
    const rows = [...(plans || [])];
    const filtered =
      planStatusFilter === "all"
        ? rows
        : rows.filter((item) => Boolean(item?.is_active) === (planStatusFilter === "active"));
    filtered.sort((a, b) => {
      const dir = planSortDir === "desc" ? -1 : 1;
      const read = (item) => {
        if (planSortBy === "name") return String(item?.name || "");
        if (planSortBy === "price") return Number(item?.monthly_price_cents || 0);
        if (planSortBy === "max_tenants") return Number(item?.max_tenants || 0);
        if (planSortBy === "max_users") return Number(item?.max_users || 0);
        if (planSortBy === "max_data_sources_per_client") return Number(item?.max_data_sources_per_client || 0);
        if (planSortBy === "max_data_sources_per_tenant") return Number(item?.max_data_sources_per_tenant || 0);
        return String(item?.code || "");
      };
      const av = read(a);
      const bv = read(b);
      if (typeof av === "number" || typeof bv === "number") {
        return (Number(av) - Number(bv)) * dir;
      }
      return String(av).localeCompare(String(bv), "pt-BR", { sensitivity: "base" }) * dir;
    });
    return filtered;
  }, [plans, planStatusFilter, planSortBy, planSortDir]);

  const runIngestion = async () => {
    try {
      await enqueueIngestionJob({});
      onSystemMessage("success", "Operacao", "Job de ingestao enfileirado.");
      await loadAll();
    } catch (error) {
      if (error?.code === "tenant_blocked") {
        onSystemMessage("warning", "Tenant bloqueado", "Tenant bloqueado por inadimplencia. Regularize o faturamento para continuar.");
      } else {
        onSystemMessage("error", "Falha job", error.message);
      }
    }
  };

  const runRag = async () => {
    try {
      await enqueueRagRebuildJob({});
      onSystemMessage("success", "Operacao", "Job de rebuild RAG enfileirado.");
      await loadAll();
    } catch (error) {
      if (error?.code === "tenant_blocked") {
        onSystemMessage("warning", "Tenant bloqueado", "Tenant bloqueado por inadimplencia. Regularize o faturamento para continuar.");
      } else {
        onSystemMessage("error", "Falha job", error.message);
      }
    }
  };

  const exportLlmCsv = async () => {
    try {
      const blob = await downloadBillingLlmUsageCsv(Number(llmUsageDays) || 30, usageTenantId || undefined);
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `llm-usage-${usageTenantId || "tenant-atual"}-${llmUsageDays}d.csv`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
      onSystemMessage("success", "Exportacao", "CSV de consumo LLM gerado com sucesso.");
    } catch (error) {
      onSystemMessage("error", "Falha exportacao", error.message);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>Faturamento (Owner)</h2>
        <p>Planos, assinatura, custos de LLM e jobs operacionais.</p>
      </header>
      <div className="page-actions">
        {isGlobalSuperadmin ? (
          <button type="button" className="btn btn-primary" onClick={openNewPlanModal}>
            Novo Plano (Superadmin)
          </button>
        ) : null}
        <button type="button" className="btn btn-primary" onClick={() => {
          setPlanCode(subscription.plan_code || "starter");
          setToleranceDays(String(subscription.tolerance_days || 5));
          setShowModal(true);
        }}>
          Atualizar Assinatura (Modal)
        </button>
        <button type="button" className="btn btn-secondary" onClick={runIngestion}>
          Enfileirar Ingestao
        </button>
        <button type="button" className="btn btn-secondary" onClick={runRag}>
          Enfileirar RAG
        </button>
        <button type="button" className="btn btn-secondary" onClick={loadAll}>
          Atualizar
        </button>
      </div>

      <section className="catalog-block">
        <h3>Assinatura ativa</h3>
        <div className="table-wrap">
          <table className="data-table"><tbody>
            <tr><th>Plano</th><td>{subscription.plan_name || "-"}</td></tr>
            <tr><th>Codigo</th><td>{subscription.plan_code || "-"}</td></tr>
            <tr><th>Max tenants</th><td>{subscription.max_tenants ?? "-"}</td></tr>
            <tr><th>Max usuarios</th><td>{subscription.max_users ?? "-"}</td></tr>
            <tr><th>Max fontes/cliente</th><td>{subscription.max_data_sources_per_client ?? "-"}</td></tr>
            <tr><th>Max fontes/tenant</th><td>{subscription.max_data_sources_per_tenant ?? "-"}</td></tr>
            <tr><th>Tolerancia (dias)</th><td>{subscription.tolerance_days || "-"}</td></tr>
            <tr><th>Status</th><td>{subscription.status || "-"}</td></tr>
          </tbody></table>
        </div>
      </section>

      <section className="catalog-block">
        <h3>Planos disponiveis</h3>
        <div className="inline-form">
          <label>
            Status
            <select value={planStatusFilter} onChange={(e) => setPlanStatusFilter(e.target.value)}>
              <option value="all">Todos</option>
              <option value="active">Ativos</option>
              <option value="inactive">Inativos</option>
            </select>
          </label>
          <label>
            Ordenar por
            <select value={planSortBy} onChange={(e) => setPlanSortBy(e.target.value)}>
              <option value="code">Codigo</option>
              <option value="name">Nome</option>
              <option value="price">Preco</option>
              <option value="max_tenants">Tenants</option>
              <option value="max_users">Usuarios</option>
              <option value="max_data_sources_per_client">Fontes/cliente</option>
              <option value="max_data_sources_per_tenant">Fontes/tenant</option>
            </select>
          </label>
          <button type="button" className="btn btn-secondary" onClick={() => setPlanSortDir((prev) => (prev === "asc" ? "desc" : "asc"))}>
            Ordem: {planSortDir === "asc" ? "Crescente" : "Decrescente"}
          </button>
        </div>
        <div className="table-wrap">
          <table className="data-table">
            <thead><tr><th>Codigo</th><th>Nome</th><th>Tenants</th><th>Usuarios</th><th>Fontes/cliente</th><th>Fontes/tenant</th><th>Preco (cent)</th><th>Status</th>{isGlobalSuperadmin ? <th>Acoes</th> : null}</tr></thead>
            <tbody>
              {displayedPlans.map((item) => (
                <tr key={item.id}>
                  <td>{item.code}</td>
                  <td>{item.name}</td>
                  <td>{item.max_tenants}</td>
                  <td>{item.max_users}</td>
                  <td>{item.max_data_sources_per_client ?? "-"}</td>
                  <td>{item.max_data_sources_per_tenant ?? "-"}</td>
                  <td>{item.monthly_price_cents}</td>
                  <td>{item.is_active ? "Ativo" : "Inativo"}</td>
                  {isGlobalSuperadmin ? (
                    <td>
                      <button type="button" className="btn btn-small btn-secondary" onClick={() => openEditPlanModal(item)}>
                        Editar
                      </button>
                      <button type="button" className="btn btn-small btn-secondary" onClick={() => removePlan(item)}>
                        Excluir
                      </button>
                    </td>
                  ) : null}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="catalog-block">
        <h3>Observabilidade</h3>
        <div className="chip-row">
          <span className="chip">Sessoes: {metrics.active_sessions || 0}</span>
          <span className="chip">Jobs inflight: {metrics.jobs_inflight || 0}</span>
          <span className="chip">Jobs falhos: {metrics.jobs_failed || 0}</span>
          <span className="chip">Incidentes abertos: {metrics.open_incidents || 0}</span>
        </div>
      </section>

      <section className="catalog-block">
        <h3>Consumo LLM do app</h3>
        <div className="inline-form">
          <label>
            Periodo (dias)
            <select value={llmUsageDays} onChange={(e) => setLlmUsageDays(e.target.value)}>
              <option value="7">7</option>
              <option value="30">30</option>
              <option value="90">90</option>
            </select>
          </label>
          {canSelectUsageTenant ? (
            <label>
              Tenant
              <select value={usageTenantId} onChange={(e) => setUsageTenantId(e.target.value)}>
                <option value="">Tenant atual</option>
                {tenantOptions.map((item) => (
                  <option key={item.id} value={String(item.id)}>
                    {item.id} - {item.name}
                  </option>
                ))}
              </select>
            </label>
          ) : null}
          <button type="button" className="btn btn-secondary" onClick={loadAll}>
            Atualizar Consumo
          </button>
          <button type="button" className="btn btn-secondary" onClick={exportLlmCsv}>
            Exportar CSV
          </button>
        </div>
        <div className="chip-row">
          <span className="chip">Chamadas: {llmUsage?.summary?.calls || 0}</span>
          <span className="chip">Input tokens: {llmUsage?.summary?.input_tokens || 0}</span>
          <span className="chip">Output tokens: {llmUsage?.summary?.output_tokens || 0}</span>
          <span className="chip">Total tokens: {llmUsage?.summary?.total_tokens || 0}</span>
          <span className="chip">Custo: {(((llmUsage?.summary?.amount_cents || 0) / 100).toFixed(2))}</span>
        </div>
        <div className="table-wrap">
          <table className="data-table">
            <thead><tr><th>Funcionalidade</th><th>Chamadas</th><th>Tokens</th><th>Custo (cent)</th></tr></thead>
            <tbody>
              {(llmUsage?.by_feature || []).map((item) => (
                <tr key={item.feature_code}>
                  <td>{item.feature_code}</td>
                  <td>{item.calls}</td>
                  <td>{item.total_tokens}</td>
                  <td>{item.amount_cents}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="catalog-block">
        <h3>Ultimos jobs assinc</h3>
        <div className="table-wrap">
          <table className="data-table">
            <thead><tr><th>ID</th><th>Tipo</th><th>Status</th><th>Criado</th></tr></thead>
            <tbody>
              {jobs.map((item) => (
                <tr key={item.id}>
                  <td>{item.id}</td>
                  <td>{item.job_kind}</td>
                  <td>{item.status}</td>
                  <td>{item.created_at || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {showModal ? (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <div className="modal-card form-modal">
            <header className="modal-header"><h3>Assinatura</h3></header>
            <div className="modal-content form-grid">
              <label>Plano
                <select value={planCode} onChange={(e) => setPlanCode(e.target.value)}>
                  {plans.map((item) => <option key={item.code} value={item.code}>{item.name}</option>)}
                </select>
              </label>
              <label>Tolerancia (dias)<input value={toleranceDays} onChange={(e) => setToleranceDays(e.target.value)} /></label>
              <div className="modal-actions">
                <button type="button" className="btn btn-secondary" onClick={() => setShowModal(false)}>Cancelar</button>
                <button type="button" className="btn btn-primary" onClick={saveSubscription}>Salvar</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {planModalOpen ? (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <div className="modal-card form-modal">
            <header className="modal-header"><h3>{planDraft.id ? "Editar Plano" : "Novo Plano"}</h3></header>
            <div className="modal-content form-grid">
              <label>Codigo
                <input value={planDraft.code} onChange={(e) => setPlanDraft((prev) => ({ ...prev, code: e.target.value }))} />
              </label>
              <label>Nome
                <input value={planDraft.name} onChange={(e) => setPlanDraft((prev) => ({ ...prev, name: e.target.value }))} />
              </label>
              <label>Max tenants
                <input type="number" min={1} value={planDraft.max_tenants} onChange={(e) => setPlanDraft((prev) => ({ ...prev, max_tenants: e.target.value }))} />
              </label>
              <label>Max usuarios
                <input type="number" min={1} value={planDraft.max_users} onChange={(e) => setPlanDraft((prev) => ({ ...prev, max_users: e.target.value }))} />
              </label>
              <label>Max fontes/cliente
                <input type="number" min={1} value={planDraft.max_data_sources_per_client} onChange={(e) => setPlanDraft((prev) => ({ ...prev, max_data_sources_per_client: e.target.value }))} />
              </label>
              <label>Max fontes/tenant
                <input type="number" min={1} value={planDraft.max_data_sources_per_tenant} onChange={(e) => setPlanDraft((prev) => ({ ...prev, max_data_sources_per_tenant: e.target.value }))} />
              </label>
              <label>Preco mensal (cent)
                <input type="number" min={0} value={planDraft.monthly_price_cents} onChange={(e) => setPlanDraft((prev) => ({ ...prev, monthly_price_cents: e.target.value }))} />
              </label>
              <label className="checkbox-inline">
                <input type="checkbox" checked={Boolean(planDraft.is_active)} onChange={(e) => setPlanDraft((prev) => ({ ...prev, is_active: e.target.checked }))} />
                Plano ativo
              </label>
              <div className="modal-actions">
                <button type="button" className="btn btn-secondary" onClick={() => setPlanModalOpen(false)}>Cancelar</button>
                <button type="button" className="btn btn-primary" onClick={savePlan}>Salvar Plano</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}

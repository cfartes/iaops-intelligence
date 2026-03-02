import { useEffect, useState } from "react";
import {
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
  upsertBillingSubscription,
} from "../api/mcpApi";

export default function BillingPanel({ onSystemMessage }) {
  const auth = getAuthContext();
  const authRole = String(auth?.role || "").toLowerCase();
  const canSelectUsageTenant = ["owner", "admin", "superadmin"].includes(authRole);
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

  const loadAll = async () => {
    try {
      const [p, s, j, m, u, t] = await Promise.all([
        listBillingPlans(),
        getBillingSubscription(),
        listAsyncJobs(20),
        getObservabilityMetrics(),
        getBillingLlmUsage(Number(llmUsageDays) || 30, usageTenantId || undefined),
        listClientTenants(),
      ]);
      setPlans(p.plans || []);
      setSubscription(s.subscription || {});
      setJobs(j.jobs || []);
      setMetrics(m || {});
      setLlmUsage(u || { summary: {}, by_feature: [], recent: [] });
      const tenants = t.tenants || [];
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
  }, [llmUsageDays]);

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
            <tr><th>Tolerancia (dias)</th><td>{subscription.tolerance_days || "-"}</td></tr>
            <tr><th>Status</th><td>{subscription.status || "-"}</td></tr>
          </tbody></table>
        </div>
      </section>

      <section className="catalog-block">
        <h3>Planos disponiveis</h3>
        <div className="table-wrap">
          <table className="data-table">
            <thead><tr><th>Codigo</th><th>Nome</th><th>Tenants</th><th>Usuarios</th><th>Preco (cent)</th></tr></thead>
            <tbody>
              {plans.map((item) => (
                <tr key={item.id}>
                  <td>{item.code}</td>
                  <td>{item.name}</td>
                  <td>{item.max_tenants}</td>
                  <td>{item.max_users}</td>
                  <td>{item.monthly_price_cents}</td>
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
    </section>
  );
}

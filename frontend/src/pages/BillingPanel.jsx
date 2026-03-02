import { useEffect, useState } from "react";
import {
  enqueueIngestionJob,
  enqueueRagRebuildJob,
  getBillingSubscription,
  getObservabilityMetrics,
  listAsyncJobs,
  listBillingPlans,
  upsertBillingSubscription,
} from "../api/mcpApi";

export default function BillingPanel({ onSystemMessage }) {
  const [plans, setPlans] = useState([]);
  const [subscription, setSubscription] = useState({});
  const [jobs, setJobs] = useState([]);
  const [metrics, setMetrics] = useState({});
  const [showModal, setShowModal] = useState(false);
  const [planCode, setPlanCode] = useState("starter");
  const [toleranceDays, setToleranceDays] = useState("5");

  const loadAll = async () => {
    try {
      const [p, s, j, m] = await Promise.all([listBillingPlans(), getBillingSubscription(), listAsyncJobs(20), getObservabilityMetrics()]);
      setPlans(p.plans || []);
      setSubscription(s.subscription || {});
      setJobs(j.jobs || []);
      setMetrics(m || {});
    } catch (error) {
      onSystemMessage("error", "Falha faturamento", error.message);
    }
  };

  useEffect(() => {
    loadAll();
  }, []);

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
      onSystemMessage("error", "Falha job", error.message);
    }
  };

  const runRag = async () => {
    try {
      await enqueueRagRebuildJob({});
      onSystemMessage("success", "Operacao", "Job de rebuild RAG enfileirado.");
      await loadAll();
    } catch (error) {
      onSystemMessage("error", "Falha job", error.message);
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


import { useEffect, useState } from "react";
import {
  getLgpdPolicy,
  listLgpdDsr,
  listLgpdRules,
  openLgpdDsr,
  resolveLgpdDsr,
  upsertLgpdPolicy,
  upsertLgpdRule,
} from "../api/mcpApi";

export default function LgpdPanel({ onSystemMessage }) {
  const [policy, setPolicy] = useState({});
  const [rules, setRules] = useState([]);
  const [requests, setRequests] = useState([]);
  const [showPolicyModal, setShowPolicyModal] = useState(false);
  const [showRuleModal, setShowRuleModal] = useState(false);
  const [showRequestModal, setShowRequestModal] = useState(false);
  const [policyDraft, setPolicyDraft] = useState({ dpo_name: "", dpo_email: "", retention_days: "", legal_notes: "" });
  const [ruleDraft, setRuleDraft] = useState({ schema_name: "public", table_name: "", column_name: "", rule_type: "mask", rule_config: "{}", is_active: true });
  const [requestDraft, setRequestDraft] = useState({ requester_name: "", requester_email: "", request_type: "access", subject_key: "", notes: "" });

  const loadAll = async () => {
    try {
      const [p, r, d] = await Promise.all([getLgpdPolicy(), listLgpdRules(), listLgpdDsr()]);
      setPolicy(p.policy || {});
      setRules(r.rules || []);
      setRequests(d.requests || []);
    } catch (error) {
      onSystemMessage("error", "Falha LGPD", error.message);
    }
  };

  useEffect(() => {
    loadAll();
  }, []);

  const savePolicy = async () => {
    try {
      await upsertLgpdPolicy({
        ...policyDraft,
        retention_days: policyDraft.retention_days ? Number(policyDraft.retention_days) : null,
      });
      setShowPolicyModal(false);
      onSystemMessage("success", "LGPD", "Politica salva com sucesso.");
      await loadAll();
    } catch (error) {
      onSystemMessage("error", "Falha LGPD", error.message);
    }
  };

  const saveRule = async () => {
    try {
      await upsertLgpdRule({
        ...ruleDraft,
        rule_config: JSON.parse(ruleDraft.rule_config || "{}"),
      });
      setShowRuleModal(false);
      onSystemMessage("success", "LGPD", "Regra salva com sucesso.");
      await loadAll();
    } catch (error) {
      onSystemMessage("error", "Falha LGPD", error.message);
    }
  };

  const openRequest = async () => {
    try {
      await openLgpdDsr(requestDraft);
      setShowRequestModal(false);
      onSystemMessage("success", "LGPD", "Solicitacao registrada.");
      await loadAll();
    } catch (error) {
      onSystemMessage("error", "Falha LGPD", error.message);
    }
  };

  const resolveRequest = async (id) => {
    try {
      await resolveLgpdDsr({ request_id: id });
      onSystemMessage("success", "LGPD", "Solicitacao marcada como resolvida.");
      await loadAll();
    } catch (error) {
      onSystemMessage("error", "Falha LGPD", error.message);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>LGPD</h2>
        <p>Politica do tenant, regras de mascaramento e solicitacoes de titular.</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-primary" onClick={() => {
          setPolicyDraft({
            dpo_name: policy.dpo_name || "",
            dpo_email: policy.dpo_email || "",
            retention_days: policy.retention_days || "",
            legal_notes: policy.legal_notes || "",
          });
          setShowPolicyModal(true);
        }}>
          Editar Politica (Modal)
        </button>
        <button type="button" className="btn btn-secondary" onClick={() => setShowRuleModal(true)}>
          Nova Regra (Modal)
        </button>
        <button type="button" className="btn btn-secondary" onClick={() => setShowRequestModal(true)}>
          Nova Solicitacao (Modal)
        </button>
        <button type="button" className="btn btn-secondary" onClick={loadAll}>
          Atualizar
        </button>
      </div>

      <section className="catalog-block">
        <h3>Politica</h3>
        <div className="table-wrap">
          <table className="data-table">
            <tbody>
              <tr><th>DPO</th><td>{policy.dpo_name || "-"}</td></tr>
              <tr><th>E-mail DPO</th><td>{policy.dpo_email || "-"}</td></tr>
              <tr><th>Retencao (dias)</th><td>{policy.retention_days || "-"}</td></tr>
              <tr><th>Notas legais</th><td>{policy.legal_notes || "-"}</td></tr>
            </tbody>
          </table>
        </div>
      </section>

      <section className="catalog-block">
        <h3>Regras de campo</h3>
        <div className="table-wrap">
          <table className="data-table">
            <thead><tr><th>ID</th><th>Campo</th><th>Tipo</th><th>Ativa</th></tr></thead>
            <tbody>
              {rules.map((item) => (
                <tr key={item.id}>
                  <td>{item.id}</td>
                  <td>{`${item.schema_name}.${item.table_name}.${item.column_name}`}</td>
                  <td>{item.rule_type}</td>
                  <td>{item.is_active ? "Sim" : "Nao"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="catalog-block">
        <h3>Solicitacoes de titular</h3>
        <div className="table-wrap">
          <table className="data-table">
            <thead><tr><th>ID</th><th>Solicitante</th><th>Tipo</th><th>Status</th><th>Acoes</th></tr></thead>
            <tbody>
              {requests.map((item) => (
                <tr key={item.id}>
                  <td>{item.id}</td>
                  <td>{item.requester_name}</td>
                  <td>{item.request_type}</td>
                  <td>{item.status}</td>
                  <td>
                    {item.status !== "resolved" ? (
                      <button type="button" className="btn btn-small btn-secondary" onClick={() => resolveRequest(item.id)}>
                        Resolver
                      </button>
                    ) : "-"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {showPolicyModal ? (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <div className="modal-card form-modal">
            <header className="modal-header"><h3>Politica LGPD</h3></header>
            <div className="modal-content form-grid">
              <label>DPO<input value={policyDraft.dpo_name} onChange={(e) => setPolicyDraft((p) => ({ ...p, dpo_name: e.target.value }))} /></label>
              <label>E-mail DPO<input value={policyDraft.dpo_email} onChange={(e) => setPolicyDraft((p) => ({ ...p, dpo_email: e.target.value }))} /></label>
              <label>Retencao (dias)<input value={policyDraft.retention_days} onChange={(e) => setPolicyDraft((p) => ({ ...p, retention_days: e.target.value }))} /></label>
              <label>Notas legais<input value={policyDraft.legal_notes} onChange={(e) => setPolicyDraft((p) => ({ ...p, legal_notes: e.target.value }))} /></label>
              <div className="modal-actions">
                <button type="button" className="btn btn-secondary" onClick={() => setShowPolicyModal(false)}>Cancelar</button>
                <button type="button" className="btn btn-primary" onClick={savePolicy}>Salvar</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {showRuleModal ? (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <div className="modal-card form-modal">
            <header className="modal-header"><h3>Regra LGPD</h3></header>
            <div className="modal-content form-grid">
              <label>Schema<input value={ruleDraft.schema_name} onChange={(e) => setRuleDraft((p) => ({ ...p, schema_name: e.target.value }))} /></label>
              <label>Tabela<input value={ruleDraft.table_name} onChange={(e) => setRuleDraft((p) => ({ ...p, table_name: e.target.value }))} /></label>
              <label>Coluna<input value={ruleDraft.column_name} onChange={(e) => setRuleDraft((p) => ({ ...p, column_name: e.target.value }))} /></label>
              <label>
                Tipo
                <select value={ruleDraft.rule_type} onChange={(e) => setRuleDraft((p) => ({ ...p, rule_type: e.target.value }))}>
                  <option value="mask">mask</option>
                  <option value="email_mask">email_mask</option>
                  <option value="hash">hash</option>
                  <option value="last4">last4</option>
                  <option value="cpf_mask">cpf_mask</option>
                  <option value="block">block</option>
                </select>
              </label>
              <label>Config JSON<input value={ruleDraft.rule_config} onChange={(e) => setRuleDraft((p) => ({ ...p, rule_config: e.target.value }))} /></label>
              <div className="modal-actions">
                <button type="button" className="btn btn-secondary" onClick={() => setShowRuleModal(false)}>Cancelar</button>
                <button type="button" className="btn btn-primary" onClick={saveRule}>Salvar</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {showRequestModal ? (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <div className="modal-card form-modal">
            <header className="modal-header"><h3>Solicitacao LGPD</h3></header>
            <div className="modal-content form-grid">
              <label>Nome<input value={requestDraft.requester_name} onChange={(e) => setRequestDraft((p) => ({ ...p, requester_name: e.target.value }))} /></label>
              <label>E-mail<input value={requestDraft.requester_email} onChange={(e) => setRequestDraft((p) => ({ ...p, requester_email: e.target.value }))} /></label>
              <label>Tipo<input value={requestDraft.request_type} onChange={(e) => setRequestDraft((p) => ({ ...p, request_type: e.target.value }))} /></label>
              <label>Chave do titular<input value={requestDraft.subject_key} onChange={(e) => setRequestDraft((p) => ({ ...p, subject_key: e.target.value }))} /></label>
              <label>Notas<input value={requestDraft.notes} onChange={(e) => setRequestDraft((p) => ({ ...p, notes: e.target.value }))} /></label>
              <div className="modal-actions">
                <button type="button" className="btn btn-secondary" onClick={() => setShowRequestModal(false)}>Cancelar</button>
                <button type="button" className="btn btn-primary" onClick={openRequest}>Abrir</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}

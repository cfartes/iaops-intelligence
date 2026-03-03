import { useEffect, useState } from "react";
import {
  getLgpdPolicy,
  listOnboardingMonitoredColumns,
  listOnboardingMonitoredTables,
  listLgpdDsr,
  listLgpdRules,
  openLgpdDsr,
  resolveLgpdDsr,
  upsertLgpdPolicy,
  upsertLgpdRule,
} from "../api/mcpApi";

export default function LgpdPanel({ onSystemMessage }) {
  const ruleTypeOptions = [
    { value: "mask", label: "Mascarar valor" },
    { value: "email_mask", label: "Ocultar e-mail" },
    { value: "hash", label: "Hash irreversivel" },
    { value: "last4", label: "Exibir apenas ultimos 4 digitos" },
    { value: "cpf_mask", label: "Mascarar CPF" },
    { value: "block", label: "Bloquear exibicao" },
  ];
  const [policy, setPolicy] = useState({});
  const [rules, setRules] = useState([]);
  const [requests, setRequests] = useState([]);
  const [showPolicyModal, setShowPolicyModal] = useState(false);
  const [showRuleModal, setShowRuleModal] = useState(false);
  const [showRequestModal, setShowRequestModal] = useState(false);
  const [monitoredTables, setMonitoredTables] = useState([]);
  const [columnsByTableId, setColumnsByTableId] = useState({});
  const [policyDraft, setPolicyDraft] = useState({ dpo_name: "", dpo_email: "", retention_days: "", legal_notes: "" });
  const [ruleDraft, setRuleDraft] = useState({
    monitored_table_id: "",
    schema_name: "",
    table_name: "",
    column_name: "",
    rule_type: "mask",
    is_active: true,
  });
  const [requestDraft, setRequestDraft] = useState({ requester_name: "", requester_email: "", request_type: "access", subject_key: "", notes: "" });

  const buildRuleConfig = (ruleType) => {
    const kind = String(ruleType || "").trim().toLowerCase();
    if (kind === "mask") return { strategy: "full_mask" };
    if (kind === "email_mask") return { strategy: "email_mask" };
    if (kind === "hash") return { strategy: "sha256" };
    if (kind === "last4") return { strategy: "last4" };
    if (kind === "cpf_mask") return { strategy: "cpf_mask" };
    if (kind === "block") return { strategy: "deny_access" };
    return {};
  };

  const loadAll = async () => {
    try {
      const [p, r, d, tableData] = await Promise.all([
        getLgpdPolicy(),
        listLgpdRules(),
        listLgpdDsr(),
        listOnboardingMonitoredTables(),
      ]);
      setPolicy(p.policy || {});
      setRules(r.rules || []);
      setRequests(d.requests || []);
      const tables = tableData.tables || [];
      setMonitoredTables(tables);

      const colEntries = await Promise.all(
        tables.map(async (table) => {
          try {
            const cols = await listOnboardingMonitoredColumns(table.id);
            return [String(table.id), cols.columns || []];
          } catch (_) {
            return [String(table.id), []];
          }
        }),
      );
      setColumnsByTableId(Object.fromEntries(colEntries));
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
      if (!ruleDraft.schema_name || !ruleDraft.table_name || !ruleDraft.column_name) {
        onSystemMessage("warning", "LGPD", "Selecione tabela e coluna para aplicar a regra.");
        return;
      }
      await upsertLgpdRule({
        schema_name: ruleDraft.schema_name,
        table_name: ruleDraft.table_name,
        column_name: ruleDraft.column_name,
        rule_type: ruleDraft.rule_type,
        rule_config: buildRuleConfig(ruleDraft.rule_type),
        is_active: Boolean(ruleDraft.is_active),
      });
      setShowRuleModal(false);
      onSystemMessage("success", "LGPD", "Regra salva com sucesso.");
      await loadAll();
    } catch (error) {
      onSystemMessage("error", "Falha LGPD", error.message);
    }
  };

  const selectedColumns = columnsByTableId[String(ruleDraft.monitored_table_id || "")] || [];

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
              <label>
                Database/Schema + Tabela
                <select
                  value={String(ruleDraft.monitored_table_id || "")}
                  onChange={(e) => {
                    const tableId = String(e.target.value || "");
                    const table = monitoredTables.find((item) => String(item.id) === tableId);
                    setRuleDraft((prev) => ({
                      ...prev,
                      monitored_table_id: tableId,
                      schema_name: table?.schema_name || "",
                      table_name: table?.table_name || "",
                      column_name: "",
                    }));
                  }}
                >
                  <option value="">Selecione...</option>
                  {monitoredTables.map((item) => (
                    <option key={item.id} value={String(item.id)}>
                      {(item.source_name || item.source_type || "Fonte")} - {item.schema_name}.{item.table_name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Coluna
                <select
                  value={ruleDraft.column_name}
                  onChange={(e) => setRuleDraft((p) => ({ ...p, column_name: e.target.value }))}
                  disabled={!ruleDraft.monitored_table_id}
                >
                  <option value="">{ruleDraft.monitored_table_id ? "Selecione..." : "Selecione uma tabela primeiro"}</option>
                  {selectedColumns.map((item) => (
                    <option key={item.id || item.column_name} value={item.column_name}>
                      {item.column_name} ({item.data_type || "n/a"})
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Tipo
                <select value={ruleDraft.rule_type} onChange={(e) => setRuleDraft((p) => ({ ...p, rule_type: e.target.value }))}>
                  {ruleTypeOptions.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
              <div className="form-note">
                A configuracao tecnica da regra sera aplicada automaticamente.
              </div>
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

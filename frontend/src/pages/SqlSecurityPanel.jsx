import { useEffect, useState } from "react";
import {
  getSqlSecurityPolicy,
  listMcpToolPolicies,
  updateMcpToolPolicy,
  updateSqlSecurityPolicy,
} from "../api/mcpApi";
import McpToolPolicyModal from "../components/McpToolPolicyModal";
import SqlSecurityPolicyModal from "../components/SqlSecurityPolicyModal";
import { tUi } from "../i18n/uiText";

export default function SqlSecurityPanel({ onSystemMessage }) {
  const [policy, setPolicy] = useState(null);
  const [toolPolicies, setToolPolicies] = useState([]);
  const [editingToolPolicy, setEditingToolPolicy] = useState(null);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);

  const loadPolicy = async () => {
    setLoading(true);
    try {
      const data = await getSqlSecurityPolicy();
      setPolicy(data.policy || null);
    } catch (error) {
      onSystemMessage("error", tUi("sql.fail.load", "Erro ao carregar politica SQL"), error.message);
    } finally {
      setLoading(false);
    }
  };

  const loadToolPolicies = async () => {
    try {
      const data = await listMcpToolPolicies();
      setToolPolicies(data.policies || []);
    } catch (error) {
      onSystemMessage("error", tUi("sql.mcp.fail.load", "Erro ao carregar policies MCP"), error.message);
    }
  };

  useEffect(() => {
    loadPolicy();
    loadToolPolicies();
  }, []);

  const savePolicy = async (payload) => {
    try {
      const data = await updateSqlSecurityPolicy(payload);
      setPolicy(data.policy || null);
      setIsModalOpen(false);
      onSystemMessage(
        "success",
        tUi("sql.ok.save.title", "Politica atualizada"),
        tUi("sql.ok.save.message", "Configuracao de Seguranca SQL salva com sucesso.")
      );
    } catch (error) {
      onSystemMessage("error", tUi("sql.fail.save", "Falha ao salvar politica SQL"), error.message);
    }
  };

  const saveToolPolicy = async (payload) => {
    try {
      await updateMcpToolPolicy(payload);
      setEditingToolPolicy(null);
      await loadToolPolicies();
      onSystemMessage(
        "success",
        tUi("sql.mcp.ok.save.title", "Policy MCP atualizada"),
        tUi("sql.mcp.ok.save.message", "Configuracao da tool MCP salva com sucesso.")
      );
    } catch (error) {
      onSystemMessage("error", tUi("sql.mcp.fail.save", "Falha ao salvar policy MCP"), error.message);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>{tUi("sql.header.title", "Seguranca SQL")}</h2>
        <p>{tUi("sql.header.subtitle", "Whitelist de schema, limites e mascaramento da tool `query.execute_safe_sql`.")}</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-primary" onClick={() => setIsModalOpen(true)} disabled={!policy}>
          {tUi("sql.edit", "Editar Politica (Modal)")}
        </button>
        <button type="button" className="btn btn-secondary" onClick={loadPolicy}>
          {tUi("sql.refresh", "Atualizar")}
        </button>
      </div>

      {loading && <p className="empty-state">{tUi("common.loading", "Carregando...")}</p>}

      {!loading && policy && (
        <div className="metric-grid">
          <article className="metric-card">
            <h4>Max rows</h4>
            <strong>{policy.max_rows ?? "n/a"}</strong>
          </article>
          <article className="metric-card">
            <h4>Max calls/min</h4>
            <strong>{policy.max_calls_per_minute ?? "n/a"}</strong>
          </article>
          <article className="metric-card">
            <h4>{tUi("sql.masking", "Mascaramento")}</h4>
            <strong>{policy.require_masking ? tUi("sql.masking.active", "ativo") : tUi("sql.masking.inactive", "inativo")}</strong>
          </article>
          <article className="metric-card">
            <h4>{tUi("sql.allowedSchemas", "Schemas permitidos")}</h4>
            <div className="chip-row">
              {(policy.allowed_schema_patterns || []).map((item) => (
                <span key={item} className="chip">
                  {item}
                </span>
              ))}
            </div>
          </article>
        </div>
      )}

      <section className="table-panel">
        <header className="table-header">
          <h3>{tUi("sql.mcp.title", "Policies MCP por Tool")}</h3>
          <button type="button" className="btn btn-secondary" onClick={loadToolPolicies}>
            {tUi("sql.mcp.refresh", "Atualizar Policies")}
          </button>
        </header>
        {toolPolicies.length === 0 ? (
          <p className="empty-state">{tUi("sql.mcp.empty", "Nenhuma policy MCP encontrada.")}</p>
        ) : (
          <div className="table-responsive">
            <table>
              <thead>
                <tr>
                  <th>Tool</th>
                  <th>Role minima</th>
                  <th>Habilitada</th>
                  <th>Max rows</th>
                  <th>Max calls/min</th>
                  <th>Acoes</th>
                </tr>
              </thead>
              <tbody>
                {toolPolicies.map((item) => (
                  <tr key={item.tool_name}>
                    <td>{item.tool_name}</td>
                    <td>{item.min_role}</td>
                    <td>{item.is_enabled ? tUi("common.yes", "Sim") : tUi("common.no", "Nao")}</td>
                    <td>{item.max_rows ?? "n/a"}</td>
                    <td>{item.max_calls_per_minute ?? "n/a"}</td>
                    <td>
                      <button type="button" className="btn btn-secondary btn-sm" onClick={() => setEditingToolPolicy(item)}>
                        {tUi("sql.mcp.edit", "Editar")}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <SqlSecurityPolicyModal
        open={isModalOpen}
        policy={policy}
        onClose={() => setIsModalOpen(false)}
        onSubmit={savePolicy}
      />
      <McpToolPolicyModal
        open={Boolean(editingToolPolicy)}
        policy={editingToolPolicy}
        onClose={() => setEditingToolPolicy(null)}
        onSubmit={saveToolPolicy}
      />
    </section>
  );
}

import { useEffect, useState } from "react";
import { getSqlSecurityPolicy, updateSqlSecurityPolicy } from "../api/mcpApi";
import SqlSecurityPolicyModal from "../components/SqlSecurityPolicyModal";
import { tUi } from "../i18n/uiText";

export default function SqlSecurityPanel({ onSystemMessage }) {
  const [policy, setPolicy] = useState(null);
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

  useEffect(() => {
    loadPolicy();
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

      <SqlSecurityPolicyModal
        open={isModalOpen}
        policy={policy}
        onClose={() => setIsModalOpen(false)}
        onSubmit={savePolicy}
      />
    </section>
  );
}

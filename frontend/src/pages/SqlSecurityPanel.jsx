import { useEffect, useState } from "react";
import { getSqlSecurityPolicy, updateSqlSecurityPolicy } from "../api/mcpApi";
import SqlSecurityPolicyModal from "../components/SqlSecurityPolicyModal";

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
      onSystemMessage("error", "Erro ao carregar politica SQL", error.message);
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
      onSystemMessage("success", "Politica atualizada", "Configuracao de Seguranca SQL salva com sucesso.");
    } catch (error) {
      onSystemMessage("error", "Falha ao salvar politica SQL", error.message);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>Seguranca SQL</h2>
        <p>Whitelist de schema, limites e mascaramento da tool `query.execute_safe_sql`.</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-primary" onClick={() => setIsModalOpen(true)} disabled={!policy}>
          Editar Politica (Modal)
        </button>
        <button type="button" className="btn btn-secondary" onClick={loadPolicy}>
          Atualizar
        </button>
      </div>

      {loading && <p className="empty-state">Carregando...</p>}

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
            <h4>Mascaramento</h4>
            <strong>{policy.require_masking ? "ativo" : "inativo"}</strong>
          </article>
          <article className="metric-card">
            <h4>Schemas permitidos</h4>
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
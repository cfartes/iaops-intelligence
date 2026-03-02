import { useEffect, useMemo, useState } from "react";

export default function SqlSecurityPolicyModal({ open, policy, onClose, onSubmit }) {
  const [maxRows, setMaxRows] = useState(policy?.max_rows ?? 200);
  const [maxCalls, setMaxCalls] = useState(policy?.max_calls_per_minute ?? 30);
  const [requireMasking, setRequireMasking] = useState(policy?.require_masking ?? true);
  const [allowedSchemas, setAllowedSchemas] = useState((policy?.allowed_schema_patterns || []).join(","));

  useEffect(() => {
    setMaxRows(policy?.max_rows ?? 200);
    setMaxCalls(policy?.max_calls_per_minute ?? 30);
    setRequireMasking(policy?.require_masking ?? true);
    setAllowedSchemas((policy?.allowed_schema_patterns || []).join(","));
  }, [policy, open]);

  const canSubmit = useMemo(() => Number(maxRows) > 0 && Number(maxCalls) > 0, [maxRows, maxCalls]);

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      max_rows: Number(maxRows),
      max_calls_per_minute: Number(maxCalls),
      require_masking: Boolean(requireMasking),
      allowed_schema_patterns: allowedSchemas
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
    });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Editar politica de Seguranca SQL</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Maximo de linhas por consulta
            <input type="number" min="1" value={maxRows} onChange={(e) => setMaxRows(e.target.value)} />
          </label>
          <label>
            Maximo de chamadas por minuto
            <input type="number" min="1" value={maxCalls} onChange={(e) => setMaxCalls(e.target.value)} />
          </label>
          <label>
            Schemas permitidos (separados por virgula)
            <input value={allowedSchemas} onChange={(e) => setAllowedSchemas(e.target.value)} />
          </label>
          <label className="checkbox-inline">
            <input type="checkbox" checked={requireMasking} onChange={(e) => setRequireMasking(e.target.checked)} />
            Exigir mascaramento
          </label>

          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit}>
              Salvar
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

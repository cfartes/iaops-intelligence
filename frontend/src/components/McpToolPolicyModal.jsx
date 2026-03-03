import { useEffect, useMemo, useState } from "react";

export default function McpToolPolicyModal({ open, policy, onClose, onSubmit }) {
  const [isEnabled, setIsEnabled] = useState(Boolean(policy?.is_enabled));
  const [maxRows, setMaxRows] = useState(policy?.max_rows ?? 1000);
  const [maxCalls, setMaxCalls] = useState(policy?.max_calls_per_minute ?? 120);
  const [requireMasking, setRequireMasking] = useState(policy?.require_masking ?? true);
  const [allowedSchemas, setAllowedSchemas] = useState((policy?.allowed_schema_patterns || []).join(","));

  useEffect(() => {
    setIsEnabled(Boolean(policy?.is_enabled));
    setMaxRows(policy?.max_rows ?? 1000);
    setMaxCalls(policy?.max_calls_per_minute ?? 120);
    setRequireMasking(policy?.require_masking ?? true);
    setAllowedSchemas((policy?.allowed_schema_patterns || []).join(","));
  }, [policy, open]);

  const canSubmit = useMemo(() => {
    if (!isEnabled) return true;
    return Number(maxRows) > 0 && Number(maxCalls) > 0;
  }, [isEnabled, maxRows, maxCalls]);

  if (!open || !policy) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      tool_name: policy.tool_name,
      is_enabled: Boolean(isEnabled),
      max_rows: isEnabled ? Number(maxRows) : null,
      max_calls_per_minute: isEnabled ? Number(maxCalls) : null,
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
          <h3>Editar policy MCP</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Tool
            <input value={policy.tool_name} readOnly />
          </label>
          <label>
            Role minima
            <input value={policy.min_role || "viewer"} readOnly />
          </label>
          <label className="checkbox-inline">
            <input type="checkbox" checked={isEnabled} onChange={(e) => setIsEnabled(e.target.checked)} />
            Tool habilitada para o tenant
          </label>
          <label>
            Maximo de linhas por execucao
            <input
              type="number"
              min="1"
              value={maxRows}
              onChange={(e) => setMaxRows(e.target.value)}
              disabled={!isEnabled}
            />
          </label>
          <label>
            Maximo de chamadas por minuto
            <input
              type="number"
              min="1"
              value={maxCalls}
              onChange={(e) => setMaxCalls(e.target.value)}
              disabled={!isEnabled}
            />
          </label>
          <label>
            Schemas permitidos (separados por virgula)
            <input value={allowedSchemas} onChange={(e) => setAllowedSchemas(e.target.value)} disabled={!isEnabled} />
          </label>
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={requireMasking}
              onChange={(e) => setRequireMasking(e.target.checked)}
              disabled={!isEnabled}
            />
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


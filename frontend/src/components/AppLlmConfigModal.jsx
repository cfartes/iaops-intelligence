import { useEffect, useMemo, useState } from "react";

const INITIAL_FORM = {
  provider_name: "",
  model_code: "",
  endpoint_url: "",
  secret_ref: "",
};

export default function AppLlmConfigModal({ open, providers, initialConfig, loading, onClose, onSubmit }) {
  const [form, setForm] = useState(INITIAL_FORM);

  useEffect(() => {
    if (!open) return;
    setForm({
      provider_name: initialConfig?.provider_name || providers?.[0]?.code || "",
      model_code: initialConfig?.model_code || "",
      endpoint_url: initialConfig?.endpoint_url || "",
      secret_ref: initialConfig?.secret_ref || "",
    });
  }, [open, initialConfig, providers]);

  const canSubmit = useMemo(() => form.provider_name.trim() && form.model_code.trim(), [form]);

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      provider_name: form.provider_name.trim().toLowerCase(),
      model_code: form.model_code.trim(),
      endpoint_url: form.endpoint_url.trim() || null,
      secret_ref: form.secret_ref.trim() || null,
    });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Configurar LLM Padrao do App (Superadmin)</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Provedor
            <select
              value={form.provider_name}
              onChange={(e) => setForm((prev) => ({ ...prev, provider_name: e.target.value }))}
            >
              {providers.map((item) => (
                <option key={item.code} value={item.code}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            Modelo
            <input
              value={form.model_code}
              onChange={(e) => setForm((prev) => ({ ...prev, model_code: e.target.value }))}
              placeholder="gpt-4.1-mini / claude-sonnet / llama3..."
            />
          </label>
          <label>
            Endpoint (opcional)
            <input
              value={form.endpoint_url}
              onChange={(e) => setForm((prev) => ({ ...prev, endpoint_url: e.target.value }))}
              placeholder="https://api.openai.com/v1 ou http://localhost:11434"
            />
          </label>
          <label>
            Secret Ref (token) (opcional)
            <input
              value={form.secret_ref}
              onChange={(e) => setForm((prev) => ({ ...prev, secret_ref: e.target.value }))}
              placeholder="secret://app/llm/provider-token"
            />
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" disabled={loading} onClick={onClose}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit || loading}>
              Salvar Configuracao
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

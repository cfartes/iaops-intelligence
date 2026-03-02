import { useEffect, useMemo, useState } from "react";

const INITIAL_FORM = {
  use_app_default_llm: false,
  provider_name: "",
  model_code: "",
  endpoint_url: "",
  secret_ref: "",
};

export default function TenantLlmConfigModal({ open, providers, initialConfig, loading, onClose, onSubmit }) {
  const [form, setForm] = useState(INITIAL_FORM);

  useEffect(() => {
    if (!open) return;
    setForm({
      use_app_default_llm: Boolean(initialConfig?.use_app_default_llm),
      provider_name: initialConfig?.provider_name || providers?.[0]?.code || "",
      model_code: initialConfig?.model_code || "",
      endpoint_url: initialConfig?.endpoint_url || "",
      secret_ref: initialConfig?.secret_ref || "",
    });
  }, [open, initialConfig, providers]);

  const canSubmit = useMemo(() => {
    if (form.use_app_default_llm) return true;
    return form.provider_name.trim().length > 0 && form.model_code.trim().length > 0;
  }, [form]);

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      use_app_default_llm: Boolean(form.use_app_default_llm),
      provider_name: form.provider_name.trim().toLowerCase() || null,
      model_code: form.model_code.trim() || null,
      endpoint_url: form.endpoint_url.trim() || null,
      secret_ref: form.secret_ref.trim() || null,
    });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Configurar LLM do Tenant</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={form.use_app_default_llm}
              onChange={(e) => setForm((prev) => ({ ...prev, use_app_default_llm: e.target.checked }))}
            />
            Usar LLM padrao do app (com cobranca por token)
          </label>
          <label>
            Provedor do tenant
            <select
              value={form.provider_name}
              onChange={(e) => setForm((prev) => ({ ...prev, provider_name: e.target.value }))}
              disabled={form.use_app_default_llm}
            >
              {providers.map((item) => (
                <option key={item.code} value={item.code}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            Modelo do tenant
            <input
              value={form.model_code}
              onChange={(e) => setForm((prev) => ({ ...prev, model_code: e.target.value }))}
              disabled={form.use_app_default_llm}
            />
          </label>
          <label>
            Endpoint do tenant
            <input
              value={form.endpoint_url}
              onChange={(e) => setForm((prev) => ({ ...prev, endpoint_url: e.target.value }))}
              disabled={form.use_app_default_llm}
            />
          </label>
          <label>
            Secret ref do tenant
            <input
              value={form.secret_ref}
              onChange={(e) => setForm((prev) => ({ ...prev, secret_ref: e.target.value }))}
              disabled={form.use_app_default_llm}
            />
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" disabled={loading} onClick={onClose}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={loading || !canSubmit}>
              Salvar
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

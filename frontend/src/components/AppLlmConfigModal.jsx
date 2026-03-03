import { useEffect, useMemo, useState } from "react";

const INITIAL_FORM = {
  provider_name: "",
  model_code: "",
  endpoint_url: "",
  secret_ref: "",
};

export default function AppLlmConfigModal({
  open,
  providers,
  modelsByProvider,
  initialConfig,
  loading,
  onClose,
  onSubmit,
  onRefreshCatalog,
}) {
  const [form, setForm] = useState(INITIAL_FORM);
  const [useCustomModel, setUseCustomModel] = useState(false);

  useEffect(() => {
    if (!open) return;
    const providerName = initialConfig?.provider_name || providers?.[0]?.code || "";
    const modelCode = initialConfig?.model_code || "";
    const providerModels = Array.isArray(modelsByProvider?.[providerName]) ? modelsByProvider[providerName] : [];
    const modelInCatalog = providerModels.some((item) => item.code === modelCode);
    setForm({
      provider_name: providerName,
      model_code: modelCode,
      endpoint_url: initialConfig?.endpoint_url || "",
      secret_ref: initialConfig?.secret_ref || "",
    });
    setUseCustomModel(Boolean(modelCode) && !modelInCatalog);
  }, [open, initialConfig, providers, modelsByProvider]);

  const modelOptions = useMemo(() => {
    const key = form.provider_name || "";
    return Array.isArray(modelsByProvider?.[key]) ? modelsByProvider[key] : [];
  }, [form.provider_name, modelsByProvider]);

  useEffect(() => {
    if (!open) return;
    if (useCustomModel) return;
    if (!form.provider_name) return;
    if (modelOptions.length === 0) {
      setUseCustomModel(true);
      return;
    }
    const hasCurrent = modelOptions.some((item) => item.code === form.model_code);
    if (!hasCurrent) {
      setForm((prev) => ({ ...prev, model_code: modelOptions[0].code }));
    }
  }, [open, form.provider_name, form.model_code, modelOptions, useCustomModel]);

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
              onChange={(e) => {
                const providerName = e.target.value;
                const providerModels = Array.isArray(modelsByProvider?.[providerName]) ? modelsByProvider[providerName] : [];
                setForm((prev) => ({
                  ...prev,
                  provider_name: providerName,
                  model_code: providerModels[0]?.code || prev.model_code || "",
                }));
                if (providerModels.length === 0) setUseCustomModel(true);
              }}
            >
              {providers.map((item) => (
                <option key={item.code} value={item.code}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <div className="modal-actions">
            <button
              type="button"
              className="btn btn-secondary btn-small"
              onClick={onRefreshCatalog}
              disabled={loading}
            >
              Atualizar catalogo de modelos
            </button>
          </div>
          <label>
            Modelo
            {useCustomModel ? (
              <input
                value={form.model_code}
                onChange={(e) => setForm((prev) => ({ ...prev, model_code: e.target.value }))}
                placeholder="Digite o nome do modelo"
              />
            ) : (
              <select
                value={form.model_code}
                onChange={(e) => setForm((prev) => ({ ...prev, model_code: e.target.value }))}
              >
                {modelOptions.map((item) => (
                  <option key={item.code} value={item.code}>
                    {item.name}
                  </option>
                ))}
              </select>
            )}
            <button
              type="button"
              className="btn btn-secondary btn-small"
              onClick={() => {
                if (useCustomModel) {
                  if (modelOptions.length > 0) {
                    setForm((prev) => ({ ...prev, model_code: modelOptions[0].code }));
                    setUseCustomModel(false);
                  }
                  return;
                }
                setUseCustomModel(true);
              }}
            >
              {useCustomModel ? "Usar lista de modelos" : "Informar modelo manualmente"}
            </button>
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

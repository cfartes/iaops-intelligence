import { useEffect, useMemo, useState } from "react";
import useModalBehavior from "./useModalBehavior";

const DEFAULT_STATE = {
  connection_name: "",
  transport_type: "http",
  endpoint_url: "",
  auth_secret_ref: "",
  is_active: true,
};

export default function McpConnectionModal({ open, initialData, onClose, onSubmit }) {
  useModalBehavior({ open, onClose });
  const [form, setForm] = useState(DEFAULT_STATE);

  useEffect(() => {
    if (!open) return;
    if (!initialData) {
      setForm(DEFAULT_STATE);
      return;
    }
    setForm({
      connection_name: initialData.connection_name || "",
      transport_type: initialData.transport_type || "http",
      endpoint_url: initialData.endpoint_url || "",
      auth_secret_ref: initialData.auth_secret_ref || "",
      is_active: Boolean(initialData.is_active),
    });
  }, [open, initialData]);

  const canSubmit = useMemo(() => {
    if (!form.connection_name.trim()) return false;
    if (!["stdio", "http", "websocket"].includes(form.transport_type)) return false;
    if ((form.transport_type === "http" || form.transport_type === "websocket") && !form.endpoint_url.trim()) return false;
    return true;
  }, [form.connection_name, form.transport_type, form.endpoint_url]);

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      connection_name: form.connection_name.trim(),
      transport_type: form.transport_type,
      endpoint_url: form.endpoint_url.trim() || null,
      auth_secret_ref: form.auth_secret_ref.trim() || null,
      is_active: Boolean(form.is_active),
    });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{initialData ? "Editar conexao MCP" : "Nova conexao MCP"}</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Nome da conexao
            <input
              value={form.connection_name}
              onChange={(e) => setForm((prev) => ({ ...prev, connection_name: e.target.value }))}
              readOnly={Boolean(initialData?.connection_name)}
            />
          </label>
          <label>
            Transporte
            <select
              value={form.transport_type}
              onChange={(e) => setForm((prev) => ({ ...prev, transport_type: e.target.value }))}
            >
              <option value="http">http</option>
              <option value="websocket">websocket</option>
              <option value="stdio">stdio</option>
            </select>
          </label>
          <label>
            Endpoint URL
            <input
              value={form.endpoint_url}
              onChange={(e) => setForm((prev) => ({ ...prev, endpoint_url: e.target.value }))}
              placeholder="https://mcp.exemplo.local/tool"
            />
          </label>
          <label>
            Secret ref
            <input
              value={form.auth_secret_ref}
              onChange={(e) => setForm((prev) => ({ ...prev, auth_secret_ref: e.target.value }))}
              placeholder="secret://tenant-x/mcp/token"
            />
          </label>
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={form.is_active}
              onChange={(e) => setForm((prev) => ({ ...prev, is_active: e.target.checked }))}
            />
            Conexao ativa
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

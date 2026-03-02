import { useEffect, useMemo, useState } from "react";

const INITIAL_FORM = {
  source_type: "",
  conn_secret_ref: "",
  is_active: true,
};

export default function DataSourceFormModal({
  open,
  sourceCatalog,
  initialData = null,
  title = "Nova fonte de dados do tenant",
  submitLabel = "Cadastrar",
  onClose,
  onSubmit,
}) {
  const [form, setForm] = useState(INITIAL_FORM);

  useEffect(() => {
    if (!open) return;
    if (initialData) {
      setForm({
        source_type: initialData.source_type || sourceCatalog?.[0]?.code || "",
        conn_secret_ref: initialData.conn_secret_ref || "",
        is_active: Boolean(initialData.is_active),
      });
      return;
    }
    setForm({
      ...INITIAL_FORM,
      source_type: sourceCatalog?.[0]?.code || "",
    });
  }, [open, sourceCatalog, initialData]);

  const canSubmit = useMemo(
    () => form.source_type.trim().length > 0 && form.conn_secret_ref.trim().length > 2,
    [form.source_type, form.conn_secret_ref]
  );

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      id: initialData?.id,
      source_type: form.source_type,
      conn_secret_ref: form.conn_secret_ref.trim(),
      is_active: form.is_active,
    });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{title}</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Tipo da fonte
            <select
              value={form.source_type}
              onChange={(e) => setForm((prev) => ({ ...prev, source_type: e.target.value }))}
            >
              {sourceCatalog.map((item) => (
                <option key={item.code} value={item.code}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            Referencia do segredo de conexao
            <input
              value={form.conn_secret_ref}
              onChange={(e) => setForm((prev) => ({ ...prev, conn_secret_ref: e.target.value }))}
              placeholder="secret://tenant-10/origem/principal"
            />
          </label>
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={form.is_active}
              onChange={(e) => setForm((prev) => ({ ...prev, is_active: e.target.checked }))}
            />
            Fonte ativa
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit}>
              {submitLabel}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

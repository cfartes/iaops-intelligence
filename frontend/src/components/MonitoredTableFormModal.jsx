import { useEffect, useMemo, useState } from "react";

const INITIAL_FORM = {
  data_source_id: "",
  schema_name: "public",
  table_name: "",
  is_active: true,
};

export default function MonitoredTableFormModal({ open, sources, defaultSourceId, onClose, onSubmit }) {
  const [form, setForm] = useState(INITIAL_FORM);

  useEffect(() => {
    if (!open) return;
    setForm({
      ...INITIAL_FORM,
      data_source_id: defaultSourceId || sources?.[0]?.id || "",
    });
  }, [open, defaultSourceId, sources]);

  const canSubmit = useMemo(
    () => Boolean(form.data_source_id) && form.schema_name.trim().length > 0 && form.table_name.trim().length > 0,
    [form.data_source_id, form.schema_name, form.table_name]
  );

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      data_source_id: Number(form.data_source_id),
      schema_name: form.schema_name.trim(),
      table_name: form.table_name.trim(),
      is_active: form.is_active,
    });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Nova tabela monitorada</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Fonte de dados
            <select
              value={form.data_source_id}
              onChange={(e) => setForm((prev) => ({ ...prev, data_source_id: e.target.value }))}
            >
              {sources.map((item) => (
                <option key={item.id} value={item.id}>
                  {(item.source_name || item.source_type) + ` (#${item.id})`}
                </option>
              ))}
            </select>
          </label>
          <label>
            Schema
            <input
              value={form.schema_name}
              onChange={(e) => setForm((prev) => ({ ...prev, schema_name: e.target.value }))}
            />
          </label>
          <label>
            Tabela
            <input
              value={form.table_name}
              onChange={(e) => setForm((prev) => ({ ...prev, table_name: e.target.value }))}
            />
          </label>
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={form.is_active}
              onChange={(e) => setForm((prev) => ({ ...prev, is_active: e.target.checked }))}
            />
            Tabela ativa
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit}>
              Cadastrar
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

import { useEffect, useMemo, useState } from "react";

const INITIAL_FORM = {
  monitored_table_id: "",
  column_name: "",
  data_type: "",
  classification: "",
  description_text: "",
};

export default function MonitoredColumnFormModal({ open, tables, defaultTableId, onClose, onSubmit }) {
  const [form, setForm] = useState(INITIAL_FORM);

  useEffect(() => {
    if (!open) return;
    setForm({
      ...INITIAL_FORM,
      monitored_table_id: defaultTableId || tables?.[0]?.id || "",
    });
  }, [open, defaultTableId, tables]);

  const canSubmit = useMemo(
    () => Boolean(form.monitored_table_id) && form.column_name.trim().length > 0,
    [form.monitored_table_id, form.column_name]
  );

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      monitored_table_id: Number(form.monitored_table_id),
      column_name: form.column_name.trim(),
      data_type: form.data_type.trim() || null,
      classification: form.classification.trim() || null,
      description_text: form.description_text.trim() || null,
    });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Nova coluna monitorada</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Tabela monitorada
            <select
              value={form.monitored_table_id}
              onChange={(e) => setForm((prev) => ({ ...prev, monitored_table_id: e.target.value }))}
            >
              {tables.map((item) => (
                <option key={item.id} value={item.id}>
                  {`${item.schema_name}.${item.table_name} (#${item.id})`}
                </option>
              ))}
            </select>
          </label>
          <label>
            Coluna
            <input
              value={form.column_name}
              onChange={(e) => setForm((prev) => ({ ...prev, column_name: e.target.value }))}
            />
          </label>
          <label>
            Tipo de dado
            <input value={form.data_type} onChange={(e) => setForm((prev) => ({ ...prev, data_type: e.target.value }))} />
          </label>
          <label>
            Classificacao
            <input
              value={form.classification}
              onChange={(e) => setForm((prev) => ({ ...prev, classification: e.target.value }))}
              placeholder="identifier, sensitive, financial..."
            />
          </label>
          <label>
            Descricao
            <input
              value={form.description_text}
              onChange={(e) => setForm((prev) => ({ ...prev, description_text: e.target.value }))}
            />
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

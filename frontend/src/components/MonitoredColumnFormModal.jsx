import { useEffect, useMemo, useState } from "react";
import useModalBehavior from "./useModalBehavior";

const INITIAL_FORM = {
  monitored_table_id: "",
  column_name: "",
  data_type: "",
  classification: "",
  description_text: "",
};

export default function MonitoredColumnFormModal({
  open,
  tables,
  defaultTableId,
  loadingColumns,
  columnOptionsByTable,
  onLoadColumns,
  onClose,
  onSubmit,
}) {
  useModalBehavior({ open, onClose });
  const [form, setForm] = useState(INITIAL_FORM);
  const [manualColumn, setManualColumn] = useState(false);
  const currentColumnOptions = useMemo(() => {
    const key = String(form.monitored_table_id || "");
    const rows = columnOptionsByTable?.[key];
    return Array.isArray(rows) ? rows : [];
  }, [columnOptionsByTable, form.monitored_table_id]);

  useEffect(() => {
    if (!open) return;
    setForm({
      ...INITIAL_FORM,
      monitored_table_id: defaultTableId || tables?.[0]?.id || "",
    });
    setManualColumn(false);
  }, [open, defaultTableId, tables]);

  useEffect(() => {
    if (!open) return;
    if (!form.monitored_table_id) return;
    onLoadColumns?.(Number(form.monitored_table_id));
  }, [open, form.monitored_table_id, onLoadColumns]);

  useEffect(() => {
    if (!open || manualColumn) return;
    if (currentColumnOptions.length === 0) return;
    const hasCurrent = currentColumnOptions.some((item) => item.column_name === form.column_name);
    if (!hasCurrent) {
      const first = currentColumnOptions[0];
      setForm((prev) => ({
        ...prev,
        column_name: first.column_name,
        data_type: first.data_type || "",
      }));
      return;
    }
    const selected = currentColumnOptions.find((item) => item.column_name === form.column_name);
    if (selected && (selected.data_type || "") !== (form.data_type || "")) {
      setForm((prev) => ({
        ...prev,
        data_type: selected.data_type || "",
      }));
    }
  }, [open, manualColumn, currentColumnOptions, form.column_name, form.data_type]);

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
          <p className="empty-state">
            Este monitoramento acompanha mudancas estruturais e regras de governanca desta coluna (tipo, classificacao e qualidade).
          </p>
          <label>
            Tabela monitorada
            <select
              value={form.monitored_table_id}
              onChange={(e) =>
                setForm((prev) => ({
                  ...prev,
                  monitored_table_id: e.target.value,
                  column_name: "",
                  data_type: "",
                }))
              }
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
            {manualColumn ? (
              <input
                value={form.column_name}
                onChange={(e) => setForm((prev) => ({ ...prev, column_name: e.target.value }))}
                placeholder="Digite o nome da coluna"
              />
            ) : (
              <select
                value={form.column_name}
                onChange={(e) => {
                  const selected = currentColumnOptions.find((item) => item.column_name === e.target.value);
                  setForm((prev) => ({
                    ...prev,
                    column_name: e.target.value,
                    data_type: selected?.data_type || prev.data_type || "",
                  }));
                }}
                disabled={loadingColumns || currentColumnOptions.length === 0}
              >
                {currentColumnOptions.length > 0 ? (
                  currentColumnOptions.map((item) => (
                    <option key={item.column_name} value={item.column_name}>
                      {item.column_name}
                    </option>
                  ))
                ) : (
                  <option value="">{loadingColumns ? "Carregando colunas..." : "Sem colunas detectadas"}</option>
                )}
              </select>
            )}
            <button
              type="button"
              className="btn btn-secondary btn-small"
              onClick={() => setManualColumn((prev) => !prev)}
            >
              {manualColumn ? "Usar lista de colunas" : "Digitar coluna manualmente"}
            </button>
          </label>
          <label>
            Tipo de dado
            <input
              value={form.data_type}
              onChange={(e) => setForm((prev) => ({ ...prev, data_type: e.target.value }))}
              placeholder="Preenchido automaticamente ao selecionar a coluna"
            />
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

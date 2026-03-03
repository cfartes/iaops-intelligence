import { useMemo, useState } from "react";
import useModalBehavior from "./useModalBehavior";

const EMPTY_FORM = {
  title: "",
  severity: "medium",
  sourceEventId: "",
};

export default function IncidentFormModal({ open, onClose, onSubmit }) {
  useModalBehavior({ open, onClose });
  const [form, setForm] = useState(EMPTY_FORM);

  const canSubmit = useMemo(() => form.title.trim().length > 2, [form.title]);

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      title: form.title,
      severity: form.severity,
      source_event_id: form.sourceEventId ? Number(form.sourceEventId) : null,
    });
    setForm(EMPTY_FORM);
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Novo incidente</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Titulo
            <input value={form.title} onChange={(e) => setForm((prev) => ({ ...prev, title: e.target.value }))} />
          </label>
          <label>
            Severidade
            <select value={form.severity} onChange={(e) => setForm((prev) => ({ ...prev, severity: e.target.value }))}>
              <option value="low">low</option>
              <option value="medium">medium</option>
              <option value="high">high</option>
              <option value="critical">critical</option>
            </select>
          </label>
          <label>
            Source Event ID (opcional)
            <input
              value={form.sourceEventId}
              onChange={(e) => setForm((prev) => ({ ...prev, sourceEventId: e.target.value }))}
            />
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit}>
              Abrir incidente
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

import { useEffect, useMemo, useState } from "react";

const NEXT_OPTIONS = {
  open: ["ack", "resolved"],
  ack: ["resolved", "closed"],
  resolved: ["closed"],
  closed: [],
};

export default function IncidentStatusModal({ open, incident, onClose, onSubmit }) {
  const [status, setStatus] = useState("ack");
  const availableStatuses = incident ? NEXT_OPTIONS[incident.status] || [] : [];

  useEffect(() => {
    if (incident?.status) {
      const next = NEXT_OPTIONS[incident.status] || [];
      setStatus(next[0] || incident.status);
    }
  }, [incident]);

  const canSubmit = useMemo(() => Boolean(incident) && Boolean(status), [incident, status]);

  if (!open || !incident) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({ incident_id: incident.incident_id, new_status: status });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Atualizar status do incidente #{incident.incident_id}</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <p>
            <strong>{incident.title}</strong>
          </p>
          <label>
            Novo status
            <select value={status} onChange={(e) => setStatus(e.target.value)} disabled={availableStatuses.length === 0}>
              {availableStatuses.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          {availableStatuses.length === 0 && <p>Nao ha transicao permitida para o status atual.</p>}
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit || availableStatuses.length === 0}>
              Confirmar
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

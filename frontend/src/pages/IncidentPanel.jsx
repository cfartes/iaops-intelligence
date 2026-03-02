import { useEffect, useState } from "react";
import { listIncidents } from "../api/mcpApi";

const STATUS_OPTIONS = ["open", "ack", "resolved", "closed"];
const SEVERITY_OPTIONS = ["low", "medium", "high", "critical"];

function nextActionsByStatus(status) {
  if (status === "open") return ["ack", "resolved"];
  if (status === "ack") return ["resolved", "closed"];
  if (status === "resolved") return ["closed"];
  return [];
}

export default function IncidentPanel({
  onOpenCreate,
  onOpenStatusModal,
  onQuickStatusUpdate,
  onSystemMessage,
  reloadSignal,
}) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);
  const [statusFilter, setStatusFilter] = useState("");
  const [severityFilter, setSeverityFilter] = useState("");

  const loadIncidents = async () => {
    setLoading(true);
    try {
      const data = await listIncidents({
        limit: 30,
        status: statusFilter || undefined,
        severity: severityFilter || undefined,
      });
      setItems(data.incidents || []);
    } catch (error) {
      onSystemMessage("error", "Erro ao carregar incidentes", error.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadIncidents();
  }, [reloadSignal]);

  return (
    <section className="page-panel">
      <header>
        <h2>Incidentes</h2>
        <p>Abertura e acompanhamento de incidentes operacionais via MCP.</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-primary" onClick={onOpenCreate}>
          Novo Incidente (Modal)
        </button>
        <button type="button" className="btn btn-secondary" onClick={loadIncidents}>
          Atualizar Lista
        </button>
      </div>

      <div className="inline-form">
        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
          <option value="">Status: todos</option>
          {STATUS_OPTIONS.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>
        <select value={severityFilter} onChange={(e) => setSeverityFilter(e.target.value)}>
          <option value="">Severidade: todas</option>
          {SEVERITY_OPTIONS.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>
        <button type="button" className="btn btn-secondary" onClick={loadIncidents}>
          Aplicar Filtro
        </button>
      </div>

      <ul className="data-list">
        {loading && <li className="empty-state">Carregando...</li>}
        {!loading && items.length === 0 && <li className="empty-state">Nenhum incidente encontrado.</li>}
        {items.map((item) => (
          <li key={item.incident_id} className="row-card">
            <div>
              <strong>#{item.incident_id}</strong> {item.title}
            </div>
            <div className="chip-row">
              <span className={`chip sev-${item.severity}`}>{item.severity}</span>
              <span className="chip">{item.status}</span>
              {nextActionsByStatus(item.status).map((nextStatus) => (
                <button
                  key={`${item.incident_id}-${nextStatus}`}
                  type="button"
                  className="btn btn-secondary btn-small"
                  onClick={() => onQuickStatusUpdate(item.incident_id, nextStatus)}
                >
                  {nextStatus}
                </button>
              ))}
              <button type="button" className="btn btn-secondary btn-small" onClick={() => onOpenStatusModal(item)}>
                Atualizar Status
              </button>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}

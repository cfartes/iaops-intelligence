import { useEffect, useState } from "react";
import { listAuditCalls } from "../api/mcpApi";

const STATUS_OPTIONS = ["success", "denied", "error", "timeout"];

export default function AuditPanel({ onSystemMessage }) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);
  const [toolFilter, setToolFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [correlationFilter, setCorrelationFilter] = useState("");

  const loadCalls = async () => {
    setLoading(true);
    try {
      const data = await listAuditCalls({
        tool_name: toolFilter || undefined,
        status: statusFilter || undefined,
        correlation_id: correlationFilter || undefined,
        limit: 30,
      });
      setItems(data.calls || []);
    } catch (error) {
      onSystemMessage("error", "Erro ao carregar auditoria", error.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadCalls();
  }, []);

  return (
    <section className="page-panel">
      <header>
        <h2>Auditoria</h2>
        <p>Rastreio de chamadas MCP por tenant.</p>
      </header>

      <div className="inline-form">
        <input placeholder="Tool (ex.: incident.list)" value={toolFilter} onChange={(e) => setToolFilter(e.target.value)} />
        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
          <option value="">Status: todos</option>
          {STATUS_OPTIONS.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>
        <input
          placeholder="Correlation ID"
          value={correlationFilter}
          onChange={(e) => setCorrelationFilter(e.target.value)}
        />
        <button type="button" className="btn btn-secondary" onClick={loadCalls}>
          Aplicar Filtro
        </button>
      </div>

      <ul className="data-list">
        {loading && <li className="empty-state">Carregando...</li>}
        {!loading && items.length === 0 && <li className="empty-state">Nenhum log encontrado.</li>}
        {items.map((item) => (
          <li key={item.call_id || `${item.correlation_id}-${item.created_at}`} className="row-card">
            <div>
              <strong>{item.tool_name}</strong>
              <div className="muted">{item.correlation_id}</div>
            </div>
            <div className="chip-row">
              <span className="chip">{item.status}</span>
              <span className="chip">{item.latency_ms ?? "n/a"} ms</span>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}
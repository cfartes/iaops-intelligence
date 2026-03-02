import { useEffect, useState } from "react";
import { listEvents } from "../api/mcpApi";

export default function EventsPanel({ onSystemMessage }) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);

  const loadEvents = async () => {
    setLoading(true);
    try {
      const data = await listEvents({ limit: 20 });
      setItems(data.events || []);
    } catch (error) {
      onSystemMessage("error", "Erro ao carregar eventos", error.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadEvents();
  }, []);

  return (
    <section className="page-panel">
      <header>
        <h2>Eventos</h2>
        <p>Ultimas mudancas estruturais detectadas no tenant.</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-secondary" onClick={loadEvents}>
          Atualizar Eventos
        </button>
      </div>

      <ul className="data-list">
        {loading && <li className="empty-state">Carregando...</li>}
        {!loading && items.length === 0 && <li className="empty-state">Nenhum evento encontrado.</li>}
        {items.map((item) => (
          <li key={item.event_id} className="row-card">
            <div>
              <strong>{item.schema_name}.{item.table_name}</strong> - {item.change_type}
            </div>
            <div className="chip-row">
              <span className={`chip sev-${item.severity}`}>{item.severity}</span>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}
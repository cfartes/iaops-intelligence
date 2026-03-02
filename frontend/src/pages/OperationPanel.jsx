import { useEffect, useState } from "react";
import { getOperationHealth } from "../api/mcpApi";

export default function OperationPanel({ onSystemMessage }) {
  const [health, setHealth] = useState(null);

  const loadHealth = async () => {
    try {
      const data = await getOperationHealth(60);
      setHealth(data);
    } catch (error) {
      onSystemMessage("error", "Erro ao carregar saude operacional", error.message);
    }
  };

  useEffect(() => {
    loadHealth();
  }, []);

  return (
    <section className="page-panel">
      <header>
        <h2>Operacao</h2>
        <p>Painel de saude operacional e canais de notificacao.</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-secondary" onClick={loadHealth}>
          Atualizar Saude
        </button>
      </div>

      {!health && <p className="empty-state">Sem dados de saude.</p>}

      {health && (
        <div className="metric-grid">
          <article className="metric-card">
            <h4>Incidentes abertos</h4>
            <strong>{health.open_incidents}</strong>
          </article>
          <article className="metric-card">
            <h4>Eventos criticos (janela)</h4>
            <strong>{health.critical_events}</strong>
          </article>
          <article className="metric-card">
            <h4>Ultima varredura</h4>
            <strong>{health.last_scan_at || "n/a"}</strong>
          </article>
          <article className="metric-card">
            <h4>Canais</h4>
            <div className="chip-row">
              {Object.entries(health.channels_health || {}).map(([name, status]) => (
                <span key={name} className="chip">{name}: {status}</span>
              ))}
            </div>
          </article>
        </div>
      )}
    </section>
  );
}
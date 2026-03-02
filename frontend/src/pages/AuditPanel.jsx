import { useEffect, useMemo, useState } from "react";
import { getAuthContext, listAuditCalls } from "../api/mcpApi";
import { tUi } from "../i18n/uiText";

const STATUS_OPTIONS = ["success", "denied", "error", "timeout"];

export default function AuditPanel({ onSystemMessage }) {
  const authContext = getAuthContext();
  const auditViewStateKey = useMemo(() => {
    const clientId = authContext?.client_id || 0;
    const tenantId = authContext?.tenant_id || 0;
    const userId = authContext?.user_id || 0;
    return `iaops_audit_view_v1:${clientId}:${tenantId}:${userId}`;
  }, [authContext?.client_id, authContext?.tenant_id, authContext?.user_id]);
  const [items, setItems] = useState([]);
  const visibleItems = useMemo(
    () => (lgpdOnly ? items.filter((item) => item.error_code === "lgpd_blocked") : items),
    [items, lgpdOnly],
  );
  const [loading, setLoading] = useState(false);
  const [toolFilter, setToolFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [correlationFilter, setCorrelationFilter] = useState("");
  const [lgpdOnly, setLgpdOnly] = useState(false);
  const [viewLoaded, setViewLoaded] = useState(false);

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
      onSystemMessage("error", tUi("audit.fail", "Erro ao carregar auditoria"), error.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(auditViewStateKey);
      if (!raw) {
        setViewLoaded(true);
        return;
      }
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") {
        setViewLoaded(true);
        return;
      }
      if (typeof parsed.toolFilter === "string") setToolFilter(parsed.toolFilter);
      if (typeof parsed.statusFilter === "string") setStatusFilter(parsed.statusFilter);
      if (typeof parsed.correlationFilter === "string") setCorrelationFilter(parsed.correlationFilter);
      if (typeof parsed.lgpdOnly === "boolean") setLgpdOnly(parsed.lgpdOnly);
    } catch (_) {
      // ignorar estado invalido
    } finally {
      setViewLoaded(true);
    }
  }, [auditViewStateKey]);

  useEffect(() => {
    if (!viewLoaded) return;
    loadCalls();
  }, [viewLoaded]);

  useEffect(() => {
    if (!viewLoaded) return;
    try {
      window.localStorage.setItem(
        auditViewStateKey,
        JSON.stringify({
          toolFilter,
          statusFilter,
          correlationFilter,
          lgpdOnly,
        }),
      );
    } catch (_) {
      // localStorage indisponivel
    }
  }, [auditViewStateKey, viewLoaded, toolFilter, statusFilter, correlationFilter, lgpdOnly]);

  return (
    <section className="page-panel">
      <header>
        <h2>{tUi("audit.header.title", "Auditoria")}</h2>
        <p>{tUi("audit.header.subtitle", "Rastreio de chamadas MCP por tenant.")}</p>
      </header>

      <div className="inline-form">
        <input placeholder={tUi("audit.tool.placeholder", "Tool (ex.: incident.list)")} value={toolFilter} onChange={(e) => setToolFilter(e.target.value)} />
        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
          <option value="">{tUi("audit.status.all", "Status: todos")}</option>
          {STATUS_OPTIONS.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>
        <input
          placeholder={tUi("audit.correlation.placeholder", "Correlation ID")}
          value={correlationFilter}
          onChange={(e) => setCorrelationFilter(e.target.value)}
        />
        <button type="button" className="btn btn-secondary" onClick={loadCalls}>
          {tUi("audit.filter.apply", "Aplicar Filtro")}
        </button>
        <button
          type="button"
          className="btn btn-secondary"
          onClick={() => {
            setLgpdOnly((prev) => !prev);
          }}
        >
          {lgpdOnly ? tUi("audit.filter.lgpd.off", "Mostrar todos") : tUi("audit.filter.lgpd.on", "Somente bloqueios LGPD")}
        </button>
      </div>

      <ul className="data-list">
        {loading && <li className="empty-state">{tUi("common.loading", "Carregando...")}</li>}
        {!loading && visibleItems.length === 0 && <li className="empty-state">{tUi("audit.empty", "Nenhum log encontrado.")}</li>}
        {visibleItems.map((item) => (
          <li key={item.call_id || `${item.correlation_id}-${item.created_at}`} className="row-card">
            <div>
              <strong>{item.tool_name}</strong>
              <div className="muted">{item.correlation_id}</div>
              {item.error_code ? <div className="muted">Erro: {item.error_code}</div> : null}
              {item.error_message ? <div className="muted">{item.error_message}</div> : null}
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

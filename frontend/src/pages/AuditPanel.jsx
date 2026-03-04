import { useEffect, useMemo, useState } from "react";
import { getAuthContext, listAuditCalls } from "../api/mcpApi";
import { tUi } from "../i18n/uiText";

const STATUS_OPTIONS = ["success", "denied", "error", "timeout"];

const TOOL_LABELS = {
  "incident.list": "Listar incidentes",
  "incident.create": "Criar incidente",
  "incident.update_status": "Atualizar status de incidente",
  "events.list": "Listar eventos",
  "inventory.list_tenant_tables": "Listar tabelas monitoradas",
  "inventory.list_table_columns": "Listar colunas monitoradas",
  "inventory.register_table": "Cadastrar tabela monitorada",
  "inventory.delete_table": "Remover tabela monitorada",
  "inventory.register_column": "Cadastrar coluna monitorada",
  "inventory.delete_column": "Remover coluna monitorada",
  "source.list_tenant": "Listar fontes de dados",
  "source.register": "Cadastrar fonte de dados",
  "source.update": "Editar fonte de dados",
  "source.update_status": "Ativar/inativar fonte de dados",
  "source.delete": "Remover fonte de dados",
  "query.execute_safe_sql": "Executar consulta do Chat BI",
  "audit.list_calls": "Consultar trilha de auditoria",
  "ops.get_health_summary": "Consultar saude operacional",
  "security_sql.get_policy": "Consultar politica de seguranca SQL",
  "security_sql.update_policy": "Atualizar politica de seguranca SQL",
  "security.mfa.get_status": "Consultar status de MFA",
  "security.mfa.begin_setup": "Iniciar configuracao de MFA",
  "security.mfa.enable": "Ativar MFA",
  "security.mfa.disable_self": "Desativar MFA",
  "security.mfa.admin_reset": "Resetar MFA de usuario",
  "tenant.list_client": "Listar tenants",
  "tenant.create": "Criar tenant",
  "tenant.update_status": "Atualizar status de tenant",
  "tenant.get_limits": "Consultar limites do plano",
};

function formatWhen(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString();
}

function statusLabel(status) {
  const value = String(status || "").toLowerCase();
  if (value === "success") return "Concluido";
  if (value === "denied") return "Bloqueado";
  if (value === "timeout") return "Expirado";
  if (value === "error") return "Erro";
  return "Indefinido";
}

function statusSummary(item) {
  const status = String(item?.status || "").toLowerCase();
  if (status === "success") return "Operacao concluida com sucesso.";
  if (status === "denied") return "Operacao bloqueada por politica de seguranca/permissao.";
  if (status === "timeout") return "Operacao nao concluiu dentro do tempo limite.";
  return "Operacao finalizada com erro.";
}

function toolLabel(toolName) {
  const key = String(toolName || "").trim();
  if (!key) return "Acao nao identificada";
  return TOOL_LABELS[key] || key;
}

export default function AuditPanel({ onSystemMessage }) {
  const authContext = getAuthContext();
  const auditViewStateKey = useMemo(() => {
    const clientId = authContext?.client_id || 0;
    const tenantId = authContext?.tenant_id || 0;
    const userId = authContext?.user_id || 0;
    return `iaops_audit_view_v1:${clientId}:${tenantId}:${userId}`;
  }, [authContext?.client_id, authContext?.tenant_id, authContext?.user_id]);
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);
  const [toolFilter, setToolFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState("");
  const [correlationFilter, setCorrelationFilter] = useState("");
  const [lgpdOnly, setLgpdOnly] = useState(false);
  const [viewLoaded, setViewLoaded] = useState(false);
  const visibleItems = useMemo(
    () => (lgpdOnly ? items.filter((item) => item.error_code === "lgpd_blocked") : items),
    [items, lgpdOnly],
  );

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
        <p>{tUi("audit.header.subtitle", "Historico das operacoes do tenant em linguagem de negocio.")}</p>
      </header>

      <div className="inline-form">
        <input placeholder={tUi("audit.tool.placeholder", "Acao (ex.: incident.list)")} value={toolFilter} onChange={(e) => setToolFilter(e.target.value)} />
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
              <strong>{toolLabel(item.tool_name)}</strong>
              <div className="muted">{statusSummary(item)}</div>
              <div className="muted">Data/hora: {formatWhen(item.created_at)}</div>
              {(item.correlation_id || item.tool_name || item.error_code || item.error_message) ? (
                <details>
                  <summary className="muted" style={{ cursor: "pointer" }}>Detalhes tecnicos</summary>
                  {item.tool_name ? <div className="muted">Tool: {item.tool_name}</div> : null}
                  {item.correlation_id ? <div className="muted">Correlation ID: {item.correlation_id}</div> : null}
                  {item.error_code ? <div className="muted">Codigo erro: {item.error_code}</div> : null}
                  {item.error_message ? <div className="muted">{item.error_message}</div> : null}
                </details>
              ) : null}
            </div>
            <div className="chip-row">
              <span className="chip">{statusLabel(item.status)}</span>
              <span className="chip">{item.latency_ms ?? "n/a"} ms</span>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}

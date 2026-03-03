import { useEffect, useMemo, useState } from "react";
import {
  listOnboardingMonitoredColumns,
  listOnboardingMonitoredTables,
  listLgpdRules,
  listTenantDataSources,
} from "../api/mcpApi";
import { tUi } from "../i18n/uiText";

function parseSourceLocation(connSecretRef) {
  const raw = String(connSecretRef || "").trim();
  if (!raw) return "";
  let parsed = null;
  try {
    if (raw.startsWith("json:")) {
      parsed = JSON.parse(raw.slice(5));
    } else if (raw.startsWith("{") && raw.endsWith("}")) {
      parsed = JSON.parse(raw);
    }
  } catch (_) {
    return "";
  }
  if (!parsed || typeof parsed !== "object") return "";
  const db = String(parsed.database || parsed.dbname || parsed.db || "").trim();
  const schema = String(parsed.schema || parsed.owner || "").trim();
  if (db && schema) return `${db}.${schema}`;
  if (db) return db;
  if (schema) return schema;
  return "";
}

export default function InventoryPanel({ onSystemMessage }) {
  const [loading, setLoading] = useState(false);
  const [tables, setTables] = useState([]);
  const [columnsByTableId, setColumnsByTableId] = useState({});
  const [lgpdRules, setLgpdRules] = useState([]);
  const [selectedTableId, setSelectedTableId] = useState("");
  const [searchTerm, setSearchTerm] = useState("");
  const [sources, setSources] = useState([]);
  const [sourceFilter, setSourceFilter] = useState("");

  const loadCatalog = async () => {
    setLoading(true);
    try {
      const [sourceData, tableData] = await Promise.all([
        listTenantDataSources(),
        listOnboardingMonitoredTables(sourceFilter || undefined),
      ]);
      const nextSources = sourceData.sources || [];
      const nextTables = tableData.tables || [];
      setSources(nextSources);
      setTables(nextTables);

      const entries = await Promise.all(
        nextTables.map(async (table) => {
          try {
            const data = await listOnboardingMonitoredColumns(table.id);
            return [String(table.id), data.columns || []];
          } catch (_) {
            return [String(table.id), []];
          }
        }),
      );
      const map = Object.fromEntries(entries);
      setColumnsByTableId(map);
      try {
        const ruleData = await listLgpdRules();
        setLgpdRules(ruleData.rules || []);
      } catch (_) {
        setLgpdRules([]);
      }

      if (nextTables.length > 0) {
        setSelectedTableId((prev) => (prev && map[String(prev)] !== undefined ? prev : String(nextTables[0].id)));
      } else {
        setSelectedTableId("");
      }
    } catch (error) {
      onSystemMessage("error", tUi("inventory.fail.tables", "Erro ao carregar catalogo"), error.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadCatalog();
  }, [sourceFilter]);

  const tableCards = useMemo(() => {
    return tables.map((table) => {
      const columns = columnsByTableId[String(table.id)] || [];
      const total = columns.length;
      const documented = columns.filter((col) =>
        Boolean(
          String(
            col.description_text || col.source_description_text || col.llm_description_suggested || "",
          ).trim(),
        ),
      ).length;
      const classified = columns.filter((col) =>
        Boolean(String(col.classification || col.llm_classification_suggested || "").trim()),
      ).length;
      const completion = total > 0 ? Math.round((documented / total) * 100) : 0;
      return {
        ...table,
        total,
        documented,
        classified,
        completion,
      };
    });
  }, [tables, columnsByTableId]);

  const filteredTables = useMemo(() => {
    const term = String(searchTerm || "").trim().toLowerCase();
    if (!term) return tableCards;
    return tableCards.filter((table) => {
      const keyParts = [
        table.schema_name,
        table.table_name,
        table.source_name,
        table.source_type,
      ]
        .map((item) => String(item || "").toLowerCase())
        .join(" ");
      if (keyParts.includes(term)) return true;
      const cols = columnsByTableId[String(table.id)] || [];
      return cols.some((col) => {
        const blob = [
          col.column_name,
          col.data_type,
          col.classification,
          col.llm_classification_suggested,
          col.description_text,
          col.source_description_text,
          col.llm_description_suggested,
        ]
          .map((item) => String(item || "").toLowerCase())
          .join(" ");
        return blob.includes(term);
      });
    });
  }, [tableCards, columnsByTableId, searchTerm]);

  const selectedTable = useMemo(
    () => filteredTables.find((item) => String(item.id) === String(selectedTableId)) || null,
    [filteredTables, selectedTableId],
  );
  const selectedColumns = useMemo(
    () => columnsByTableId[String(selectedTable?.id || "")] || [],
    [columnsByTableId, selectedTable?.id],
  );
  const lgpdRuleMap = useMemo(() => {
    const map = new Map();
    for (const rule of lgpdRules || []) {
      if (!rule || !rule.is_active) continue;
      const key = [
        String(rule.schema_name || "").trim().toLowerCase(),
        String(rule.table_name || "").trim().toLowerCase(),
        String(rule.column_name || "").trim().toLowerCase(),
      ].join(".");
      if (!key || key === "..") continue;
      map.set(key, rule);
    }
    return map;
  }, [lgpdRules]);

  const totalProtectedCount = useMemo(() => {
    if (!filteredTables.length) return 0;
    let total = 0;
    for (const table of filteredTables) {
      const schema = String(table.schema_name || "").trim().toLowerCase();
      const tableName = String(table.table_name || "").trim().toLowerCase();
      const cols = columnsByTableId[String(table.id)] || [];
      for (const col of cols) {
        const key = `${schema}.${tableName}.${String(col.column_name || "").trim().toLowerCase()}`;
        if (lgpdRuleMap.has(key)) total += 1;
      }
    }
    return total;
  }, [filteredTables, columnsByTableId, lgpdRuleMap]);

  const resolveLgpdStatus = (table, columnName) => {
    if (!table) return { label: "-", variant: "none" };
    const schema = String(table.schema_name || "").trim().toLowerCase();
    const tableName = String(table.table_name || "").trim().toLowerCase();
    const col = String(columnName || "").trim().toLowerCase();
    const rule = lgpdRuleMap.get(`${schema}.${tableName}.${col}`);
    if (!rule) return { label: "-", variant: "none" };
    const kind = String(rule.rule_type || "regra").trim().toLowerCase();
    const labels = {
      mask: "Mascaramento",
      masking: "Mascaramento",
      anonymize: "Anonimizacao",
      anonymization: "Anonimizacao",
      block: "Bloqueio",
      deny: "Bloqueio",
      redact: "Redacao",
    };
    const variants = {
      mask: "mask",
      masking: "mask",
      anonymize: "anonymize",
      anonymization: "anonymize",
      block: "block",
      deny: "block",
      redact: "redact",
    };
    return { label: `${labels[kind] || kind} (ativo)`, variant: variants[kind] || "other" };
  };

  return (
    <section className="page-panel">
      <header>
        <h2>{tUi("inventory.header.title", "Catalogo de Dados")}</h2>
        <p>
          {tUi(
            "inventory.header.subtitle",
            "Visao de negocio das tabelas monitoradas, com descricoes e classificacoes para usuarios nao tecnicos.",
          )}
        </p>
      </header>
      <div className="inline-form">
        <input
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          placeholder="Buscar tabela, campo, descricao ou classificacao"
        />
        <select value={sourceFilter} onChange={(e) => setSourceFilter(e.target.value)}>
          <option value="">Todas as fontes</option>
          {sources.map((item) => (
            <option key={item.id} value={String(item.id)}>
              {parseSourceLocation(item.conn_secret_ref) || item.source_name || item.source_type}
            </option>
          ))}
        </select>
        <button type="button" className="btn btn-primary" onClick={loadCatalog} disabled={loading}>
          {loading ? "Atualizando..." : "Atualizar Catalogo"}
        </button>
      </div>

      <div className="metric-grid">
        <article className="metric-card">
          <h4>Tabelas monitoradas</h4>
          <strong>{filteredTables.length}</strong>
        </article>
        <article className="metric-card">
          <h4>Colunas monitoradas</h4>
          <strong>{filteredTables.reduce((acc, item) => acc + item.total, 0)}</strong>
        </article>
        <article className="metric-card">
          <h4>Colunas documentadas</h4>
          <strong>{filteredTables.reduce((acc, item) => acc + item.documented, 0)}</strong>
        </article>
        <article className="metric-card">
          <h4>Colunas classificadas</h4>
          <strong>{filteredTables.reduce((acc, item) => acc + item.classified, 0)}</strong>
        </article>
        <article className="metric-card">
          <h4>Campos com regra LGPD</h4>
          <strong>{totalProtectedCount}</strong>
        </article>
      </div>

      <div className="list-grid" style={{ marginTop: 16 }}>
        <div>
          <h3>{tUi("inventory.tables", "Tabelas Monitoradas")}</h3>
          <ul className="data-list">
            {loading && <li className="empty-state">Carregando catalogo...</li>}
            {!loading && filteredTables.length === 0 && <li className="empty-state">Nenhuma tabela encontrada.</li>}
            {filteredTables.map((item) => (
              <li key={`${item.id}`}>
                <button
                  type="button"
                  className={selectedTableId === String(item.id) ? "list-item active" : "list-item"}
                  onClick={() => setSelectedTableId(String(item.id))}
                >
                  <strong>{item.schema_name}.{item.table_name}</strong>
                  <div className="muted">{item.source_name || item.source_type}</div>
                  <div className="chip-row">
                    <span className="chip">Colunas: {item.total}</span>
                    <span className="chip">Doc: {item.documented}</span>
                    <span className="chip">Classif: {item.classified}</span>
                    <span className="chip">Completude: {item.completion}%</span>
                  </div>
                </button>
              </li>
            ))}
          </ul>
        </div>

        <div>
          <div className="section-header">
            <h3>{tUi("inventory.columns", "Campos da Tabela")}</h3>
          </div>
          {!selectedTable ? (
            <p className="empty-state">Selecione uma tabela para ver os campos.</p>
          ) : (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Campo</th>
                    <th>Tipo</th>
                    <th>Classificacao</th>
                    <th>Status LGPD</th>
                    <th>Descricao</th>
                  </tr>
                </thead>
                <tbody>
                  {selectedColumns.length === 0 ? (
                    <tr>
                      <td colSpan={5}>Nenhum campo monitorado para esta tabela.</td>
                    </tr>
                  ) : (
                    selectedColumns.map((col) => {
                      const lgpd = resolveLgpdStatus(selectedTable, col.column_name);
                      return (
                        <tr key={col.id || col.column_name}>
                          <td>{col.column_name}</td>
                          <td>{col.data_type || "-"}</td>
                          <td>{col.classification || col.llm_classification_suggested || "-"}</td>
                          <td>
                            <span className={`chip lgpd-chip lgpd-${lgpd.variant}`}>{lgpd.label}</span>
                          </td>
                          <td>{col.description_text || col.source_description_text || col.llm_description_suggested || "-"}</td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}

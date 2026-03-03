import { useEffect, useMemo, useState } from "react";
import {
  confirmMonitoredColumnDescription,
  listOnboardingMonitoredColumns,
  listOnboardingMonitoredTables,
  listTenantDataSources,
  updateMonitoredColumn,
} from "../api/mcpApi";
import useModalBehavior from "../components/useModalBehavior";

function parseSourceLocation(connSecretRef) {
  const raw = String(connSecretRef || "").trim();
  if (!raw) return "-";
  let parsed = null;
  try {
    if (raw.startsWith("json:")) {
      parsed = JSON.parse(raw.slice(5));
    } else if (raw.startsWith("{") && raw.endsWith("}")) {
      parsed = JSON.parse(raw);
    }
  } catch (_) {
    return "-";
  }
  if (!parsed || typeof parsed !== "object") return "-";
  const db = String(parsed.database || parsed.dbname || parsed.db || "").trim();
  const schema = String(parsed.schema || parsed.owner || "").trim();
  if (db && schema) return `${db}.${schema}`;
  if (db) return db;
  if (schema) return schema;
  return "-";
}

function isGenericDescription(text) {
  const value = String(text || "").toLowerCase();
  return value.includes("informacao de ") || value.includes("campo ") || value.includes("data de referencia");
}

function detectStatus(row) {
  const llm = String(row.llm_description_suggested || "").trim();
  const current = String(row.description_text || "").trim();
  if (row.llm_description_confirmed || (llm && current && llm === current)) return "confirmado";
  if (llm && current && llm !== current) return "rejeitado";
  return "pendente";
}

function detectConfidence(row) {
  const raw = row.llm_confidence_score;
  if (raw !== null && raw !== undefined && raw !== "") {
    const value = Number(raw);
    if (Number.isFinite(value)) {
      if (value >= 0.75) return "alta";
      if (value >= 0.5) return "media";
      return "baixa";
    }
  }
  let score = 0;
  if (String(row.llm_description_suggested || "").trim()) score += 0.35;
  if (String(row.llm_classification_suggested || row.classification || "").trim()) score += 0.25;
  if (String(row.source_description_text || "").trim()) score += 0.2;
  if (String(row.data_type || "").trim()) score += 0.1;
  if (!isGenericDescription(row.llm_description_suggested)) score += 0.1;
  if (score >= 0.75) return "alta";
  if (score >= 0.5) return "media";
  return "baixa";
}

function EditSuggestionModal({ row, saving, onCancel, onSave }) {
  const [description, setDescription] = useState("");
  const [classification, setClassification] = useState("");
  useModalBehavior({ open: Boolean(row), onClose: saving ? undefined : onCancel });

  useEffect(() => {
    if (!row) return;
    setDescription(String(row.description_text || row.llm_description_suggested || "").trim());
    setClassification(String(row.classification || row.llm_classification_suggested || "").trim());
  }, [row]);

  if (!row) return null;

  return (
    <div className="modal-overlay" onClick={() => !saving && onCancel()}>
      <div className="modal-card" onClick={(event) => event.stopPropagation()}>
        <header className="modal-header">
          <h3>Editar sugestao ({row.schema_name}.{row.table_name}.{row.column_name})</h3>
        </header>
        <div className="modal-content">
          <div className="form-grid">
            <label>
              Classificacao
              <input
                value={classification}
                onChange={(event) => setClassification(event.target.value)}
                placeholder="identifier, temporal, financial..."
              />
            </label>
            <label>
              Descricao final
              <textarea
                rows={4}
                value={description}
                onChange={(event) => setDescription(event.target.value)}
                placeholder="Descricao amigavel para usuario de negocio"
              />
            </label>
          </div>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onCancel} disabled={saving}>
              Cancelar
            </button>
            <button
              type="button"
              className="btn btn-primary"
              onClick={() => onSave({ description_text: description, classification })}
              disabled={saving}
            >
              {saving ? "Salvando..." : "Salvar edicao"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function SuggestionsPanel({ onSystemMessage }) {
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [rows, setRows] = useState([]);
  const [sources, setSources] = useState([]);
  const [sourceFilter, setSourceFilter] = useState("all");
  const [tableFilter, setTableFilter] = useState("all");
  const [dataTypeFilter, setDataTypeFilter] = useState("all");
  const [statusFilter, setStatusFilter] = useState("all");
  const [confidenceFilter, setConfidenceFilter] = useState("all");
  const [searchText, setSearchText] = useState("");
  const [editingRow, setEditingRow] = useState(null);

  const loadSuggestions = async () => {
    setLoading(true);
    try {
      const sourceResp = await listTenantDataSources();
      const sourceRows = Array.isArray(sourceResp.sources) ? sourceResp.sources : [];
      setSources(
        sourceRows.map((source) => ({
          id: source.id,
          name: parseSourceLocation(source.conn_secret_ref) !== "-" ? parseSourceLocation(source.conn_secret_ref) : source.source_name || source.source_type,
        }))
      );
      const tablePayloads = await Promise.all(
        sourceRows.map(async (source) => {
          try {
            const res = await listOnboardingMonitoredTables(source.id);
            const tables = Array.isArray(res.tables) ? res.tables : [];
            return tables.map((table) => ({
              ...table,
              source_id: Number(source.id),
              source_name: source.source_name || source.source_type,
              source_label: parseSourceLocation(source.conn_secret_ref) !== "-" ? parseSourceLocation(source.conn_secret_ref) : source.source_name || source.source_type,
            }));
          } catch (_) {
            return [];
          }
        })
      );
      const tableRows = tablePayloads.flat();
      const columnPayloads = await Promise.all(
        tableRows.map(async (table) => {
          try {
            const res = await listOnboardingMonitoredColumns(table.id);
            const cols = Array.isArray(res.columns) ? res.columns : [];
            return cols.map((column) => ({
              ...column,
              source_id: table.source_id,
              source_name: table.source_name,
              source_label: table.source_label,
              schema_name: table.schema_name,
              table_name: table.table_name,
              monitored_table_id: table.id,
              status_suggestion: detectStatus(column),
              confidence_bucket: detectConfidence(column),
            }));
          } catch (_) {
            return [];
          }
        })
      );
      setRows(columnPayloads.flat());
    } catch (error) {
      onSystemMessage("error", "Erro em sugestoes", error.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadSuggestions();
  }, []);

  const tableOptions = useMemo(() => {
    const map = new Map();
    rows.forEach((row) => {
      if (sourceFilter !== "all" && String(row.source_id) !== String(sourceFilter)) return;
      const key = `${row.schema_name}.${row.table_name}`;
      if (!map.has(key)) map.set(key, key);
    });
    return Array.from(map.values()).sort();
  }, [rows, sourceFilter]);

  useEffect(() => {
    if (tableFilter !== "all" && !tableOptions.includes(tableFilter)) {
      setTableFilter("all");
    }
  }, [tableFilter, tableOptions]);

  const dataTypes = useMemo(() => Array.from(new Set(rows.map((row) => String(row.data_type || "").trim()).filter(Boolean))).sort(), [rows]);

  const filteredRows = useMemo(() => {
    const needle = String(searchText || "").trim().toLowerCase();
    return rows.filter((row) => {
      if (sourceFilter !== "all" && String(row.source_id) !== String(sourceFilter)) return false;
      if (tableFilter !== "all" && `${row.schema_name}.${row.table_name}` !== tableFilter) return false;
      if (dataTypeFilter !== "all" && String(row.data_type || "") !== dataTypeFilter) return false;
      if (statusFilter !== "all" && row.status_suggestion !== statusFilter) return false;
      if (confidenceFilter !== "all" && row.confidence_bucket !== confidenceFilter) return false;
      if (!needle) return true;
      const text = [
        row.source_label,
        row.schema_name,
        row.table_name,
        row.column_name,
        row.data_type,
        row.llm_description_suggested,
        row.description_text,
      ]
        .map((item) => String(item || "").toLowerCase())
        .join(" ");
      return text.includes(needle);
    });
  }, [rows, sourceFilter, tableFilter, dataTypeFilter, statusFilter, confidenceFilter, searchText]);

  const refreshAndMessage = async (kind, title, message) => {
    onSystemMessage(kind, title, message);
    await loadSuggestions();
  };

  const handleConfirm = async (row) => {
    setSaving(true);
    try {
      await confirmMonitoredColumnDescription({ monitored_column_id: row.id });
      await refreshAndMessage("success", "Sugestao confirmada", `Descricao LLM confirmada para ${row.column_name}.`);
    } catch (error) {
      onSystemMessage("error", "Falha ao confirmar", error.message);
    } finally {
      setSaving(false);
    }
  };

  const handleReject = async (row) => {
    const fallback = String(row.source_description_text || row.description_text || "").trim();
    if (!fallback) {
      onSystemMessage("warning", "Rejeicao sem texto", "Edite a descricao antes de rejeitar a sugestao.");
      return;
    }
    setSaving(true);
    try {
      await updateMonitoredColumn({
        monitored_column_id: row.id,
        classification: row.classification || row.llm_classification_suggested || null,
        description_text: fallback,
        llm_description_confirmed: false,
      });
      await refreshAndMessage("success", "Sugestao rejeitada", `Sugestao da LLM rejeitada para ${row.column_name}.`);
    } catch (error) {
      onSystemMessage("error", "Falha ao rejeitar", error.message);
    } finally {
      setSaving(false);
    }
  };

  const handleSaveEdit = async ({ description_text, classification }) => {
    if (!editingRow) return;
    setSaving(true);
    try {
      await updateMonitoredColumn({
        monitored_column_id: editingRow.id,
        description_text,
        classification,
        llm_description_confirmed: false,
      });
      setEditingRow(null);
      await refreshAndMessage("success", "Sugestao atualizada", `Edicao salva para ${editingRow.column_name}.`);
    } catch (error) {
      onSystemMessage("error", "Falha ao editar", error.message);
    } finally {
      setSaving(false);
    }
  };

  const handleConfirmBatch = async () => {
    const targets = filteredRows.filter((row) => row.status_suggestion === "pendente" && String(row.llm_description_suggested || "").trim());
    if (targets.length === 0) {
      onSystemMessage("warning", "Lote vazio", "Nao ha sugestoes pendentes no filtro atual.");
      return;
    }
    setSaving(true);
    let ok = 0;
    try {
      for (const row of targets) {
        try {
          await confirmMonitoredColumnDescription({ monitored_column_id: row.id });
          ok += 1;
        } catch (_) {
          // segue com as demais
        }
      }
      await refreshAndMessage(ok > 0 ? "success" : "warning", "Confirmacao em lote", `${ok} de ${targets.length} sugestao(oes) confirmada(s).`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>Sugestoes</h2>
        <p>Curadoria das sugestoes da LLM por tabela/campo, com status e aprovacao em lote.</p>
      </header>

      <div className="inline-form" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))" }}>
        <input placeholder="Buscar campo/descricao" value={searchText} onChange={(event) => setSearchText(event.target.value)} />
        <select value={sourceFilter} onChange={(event) => setSourceFilter(event.target.value)}>
          <option value="all">Todas as fontes</option>
          {sources.map((source) => (
            <option key={source.id} value={source.id}>
              {source.name}
            </option>
          ))}
        </select>
        <select value={tableFilter} onChange={(event) => setTableFilter(event.target.value)}>
          <option value="all">Schema/Tabela: todas</option>
          {tableOptions.map((tableName) => (
            <option key={tableName} value={tableName}>
              {tableName}
            </option>
          ))}
        </select>
        <select value={dataTypeFilter} onChange={(event) => setDataTypeFilter(event.target.value)}>
          <option value="all">Tipo de dado: todos</option>
          {dataTypes.map((dataType) => (
            <option key={dataType} value={dataType}>
              {dataType}
            </option>
          ))}
        </select>
        <select value={confidenceFilter} onChange={(event) => setConfidenceFilter(event.target.value)}>
          <option value="all">Confianca: todas</option>
          <option value="alta">Alta</option>
          <option value="media">Media</option>
          <option value="baixa">Baixa</option>
        </select>
        <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
          <option value="all">Status: todos</option>
          <option value="pendente">Pendente</option>
          <option value="confirmado">Confirmado</option>
          <option value="rejeitado">Rejeitado</option>
        </select>
      </div>

      <div className="page-actions">
        <button type="button" className="btn btn-secondary" onClick={loadSuggestions} disabled={loading || saving}>
          {loading ? "Atualizando..." : "Atualizar"}
        </button>
        <button type="button" className="btn btn-primary" onClick={handleConfirmBatch} disabled={saving || loading}>
          Confirmar em lote
        </button>
      </div>

      <div className="catalog-block">
        {loading ? (
          <p className="empty-state">Carregando sugestoes...</p>
        ) : filteredRows.length === 0 ? (
          <p className="empty-state">Nenhuma sugestao encontrada para os filtros selecionados.</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Fonte</th>
                  <th>Schema</th>
                  <th>Tabela</th>
                  <th>Campo</th>
                  <th>Tipo</th>
                  <th>Confianca</th>
                  <th>Status</th>
                  <th>Classificacao</th>
                  <th>Descricao LLM</th>
                  <th>Descricao final</th>
                  <th>Acoes</th>
                </tr>
              </thead>
              <tbody>
                {filteredRows.map((row) => (
                  <tr key={row.id}>
                    <td>{row.source_label || row.source_name}</td>
                    <td>{row.schema_name}</td>
                    <td>{row.table_name}</td>
                    <td>{row.column_name}</td>
                    <td>{row.data_type || "-"}</td>
                    <td>
                      {row.confidence_bucket}
                      {row.llm_confidence_score !== null && row.llm_confidence_score !== undefined ? ` (${Math.round(Number(row.llm_confidence_score) * 100)}%)` : ""}
                    </td>
                    <td>{row.status_suggestion}</td>
                    <td>{row.llm_classification_suggested || row.classification || "-"}</td>
                    <td>{row.llm_description_suggested || "-"}</td>
                    <td>{row.description_text || "-"}</td>
                    <td>
                      <div className="chip-row">
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() => handleConfirm(row)}
                          disabled={saving || !String(row.llm_description_suggested || "").trim()}
                        >
                          Confirmar
                        </button>
                        <button type="button" className="btn btn-small btn-secondary" onClick={() => setEditingRow(row)} disabled={saving}>
                          Editar
                        </button>
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() => handleReject(row)}
                          disabled={saving || !String(row.llm_description_suggested || "").trim()}
                        >
                          Rejeitar
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <EditSuggestionModal row={editingRow} saving={saving} onCancel={() => !saving && setEditingRow(null)} onSave={handleSaveEdit} />
    </section>
  );
}

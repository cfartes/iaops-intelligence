import { useEffect, useState } from "react";
import {
  deleteDataSource,
  discoverDataSourceColumns,
  discoverDataSourceTables,
  deleteMonitoredTable,
  enrichMonitoredColumns,
  getTenantLimits,
  listOnboardingMonitoredTables,
  listSourceCatalog,
  listTenantDataSources,
  registerDataSource,
  registerMonitoredColumn,
  registerMonitoredTable,
  updateDataSource,
  updateDataSourceStatus,
  testDataSourceConnection,
} from "../api/mcpApi";
import ConfirmActionModal from "../components/ConfirmActionModal";
import DataSourceFormModal from "../components/DataSourceFormModal";
import MonitoredTableFormModal from "../components/MonitoredTableFormModal";
import { tUi } from "../i18n/uiText";

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

function translateSourceLimitMessage(rawMessage) {
  const text = String(rawMessage || "").trim();
  const normalized = text.toLowerCase();
  if (normalized.includes("limite de fontes por cliente") || normalized.includes("limite de fontes ativas por cliente")) {
    return `${tUi("onboarding.limit.sourceClient", "Limite de fontes ativas por cliente atingido no plano atual.")} ${tUi("onboarding.limit.actionHint", "Inative uma fonte existente ou altere o plano para continuar.")}`;
  }
  if (normalized.includes("limite de fontes por tenant") || normalized.includes("limite de fontes ativas por tenant")) {
    return `${tUi("onboarding.limit.sourceTenant", "Limite de fontes ativas por tenant atingido no plano atual.")} ${tUi("onboarding.limit.actionHint", "Inative uma fonte existente ou altere o plano para continuar.")}`;
  }
  return text;
}

export default function OnboardingPanel({ onSystemMessage }) {
  const [sources, setSources] = useState([]);
  const [tenantSources, setTenantSources] = useState([]);
  const [loading, setLoading] = useState(false);
  const [registering, setRegistering] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingSource, setEditingSource] = useState(null);
  const [testingSource, setTestingSource] = useState(false);
  const [monitoredTables, setMonitoredTables] = useState([]);
  const [selectedSourceIdForTables, setSelectedSourceIdForTables] = useState("");
  const [tablesLoading, setTablesLoading] = useState(false);
  const [isTableModalOpen, setIsTableModalOpen] = useState(false);
  const [tableSaving, setTableSaving] = useState(false);
  const [exportingSourceId, setExportingSourceId] = useState(null);
  const [pendingAction, setPendingAction] = useState(null);
  const [actionLoading, setActionLoading] = useState(false);
  const [limits, setLimits] = useState(null);

  const loadCatalog = async () => {
    setLoading(true);
    try {
      const data = await listSourceCatalog();
      setSources(data.sources || []);
    } catch (error) {
      onSystemMessage("error", tUi("onboarding.fail.catalog", "Erro no onboarding"), error.message);
    } finally {
      setLoading(false);
    }
  };

  const loadTenantSources = async () => {
    try {
      const [data, limitsData] = await Promise.all([listTenantDataSources(), getTenantLimits()]);
      const rows = data.sources || [];
      setTenantSources(rows);
      setLimits(limitsData?.limits || null);
      if (rows.length === 0) {
        setSelectedSourceIdForTables("");
        setMonitoredTables([]);
        return;
      }
      const selectedExists = rows.some((item) => String(item.id) === String(selectedSourceIdForTables));
      if (!selectedSourceIdForTables || !selectedExists) {
        setSelectedSourceIdForTables(String(rows[0].id));
      }
    } catch (error) {
      onSystemMessage("error", tUi("onboarding.fail.tenantSources", "Erro ao carregar fontes do tenant"), error.message);
    }
  };

  const loadMonitoredTables = async (sourceIdRaw, options = {}) => {
    const { attemptSync = true, forceSync = false } = options;
    const sourceId = sourceIdRaw || selectedSourceIdForTables || undefined;
    setTablesLoading(true);
    try {
      const data = await listOnboardingMonitoredTables(sourceId ? Number(sourceId) : undefined);
      const rows = data.tables || [];
      if (sourceId && (forceSync || (attemptSync && rows.length === 0))) {
        const source = tenantSources.find((item) => String(item.id) === String(sourceId));
        if (source && source.conn_secret_ref) {
          const discovered = await discoverDataSourceTables({
            source_type: source.source_type,
            conn_secret_ref: source.conn_secret_ref,
          });
          const discoveredTables = Array.isArray(discovered.tables) ? discovered.tables : [];
          if (discoveredTables.length > 0) {
            for (const item of discoveredTables.slice(0, 500)) {
              try {
                await registerMonitoredTable({
                  data_source_id: Number(sourceId),
                  schema_name: item.schema_name || "public",
                  table_name: item.table_name,
                  is_active: true,
                });
              } catch (_) {
                // ignora tabela ja cadastrada ou erro pontual para seguir com a sincronizacao
              }
            }
            const refreshed = await listOnboardingMonitoredTables(Number(sourceId));
            const refreshedRows = refreshed.tables || [];
            setMonitoredTables(refreshedRows);
            for (const table of refreshedRows.slice(0, 300)) {
              try {
                await syncColumnsForTable(table);
              } catch (_) {
                // segue sincronizacao parcial
              }
            }
            onSystemMessage("success", "Tabelas sincronizadas", `${refreshedRows.length} tabela(s) carregada(s) da fonte.`);
            return;
          }
        }
      }
      setMonitoredTables(rows);
    } catch (error) {
      onSystemMessage("error", tUi("onboarding.fail.tables", "Erro ao carregar tabelas monitoradas"), error.message);
    } finally {
      setTablesLoading(false);
    }
  };

  const syncColumnsForTable = async (table) => {
    if (!table) return 0;
    const source = tenantSources.find((item) => Number(item.id) === Number(table.data_source_id));
    if (!source || !source.conn_secret_ref) return 0;
    const discovered = await discoverDataSourceColumns({
      source_type: source.source_type,
      conn_secret_ref: source.conn_secret_ref,
      schema_name: table.schema_name,
      table_name: table.table_name,
    });
    const columns = Array.isArray(discovered.columns) ? discovered.columns : [];
    let synced = 0;
    for (const col of columns) {
      const columnName = String(col.column_name || "").trim();
      if (!columnName) continue;
      try {
        await registerMonitoredColumn({
          monitored_table_id: Number(table.id),
          column_name: columnName,
          data_type: col.data_type || null,
          classification: null,
          description_text: null,
        });
        synced += 1;
      } catch (_) {
        // ignora colunas ja existentes
      }
    }
    try {
      await enrichMonitoredColumns({
        monitored_table_id: Number(table.id),
        source_type: source.source_type,
        conn_secret_ref: source.conn_secret_ref,
        schema_name: table.schema_name,
        table_name: table.table_name,
      });
    } catch (_) {
      // enriquecimento e best effort; nao bloqueia sincronizacao
    }
    return synced;
  };

  const handleRegisterSource = async (payload) => {
    setRegistering(true);
    try {
      const data = await registerDataSource(payload);
      setIsModalOpen(false);
      onSystemMessage(
        "success",
        tUi("onboarding.ok.sourceCreated.title", "Fonte cadastrada"),
        `Fonte ${data.source.source_name || data.source.source_type} cadastrada com sucesso.`
      );
      await loadTenantSources();
    } catch (error) {
      onSystemMessage("error", tUi("onboarding.fail.sourceCreate", "Falha no cadastro da fonte"), translateSourceLimitMessage(error.message));
    } finally {
      setRegistering(false);
    }
  };

  const handleTestSourceConnection = async (payload) => {
    setTestingSource(true);
    try {
      const data = await testDataSourceConnection(payload);
      if (data.ok) {
        onSystemMessage(
          "success",
          "Conexao validada",
          `${data.message || "Conexao testada com sucesso."} Clique em "Atualizar" na secao de tabelas para sincronizacao automatica.`
        );
      } else {
        onSystemMessage("warning", "Falha na conexao", data.message || "Nao foi possivel validar a conexao.");
      }
    } catch (error) {
      onSystemMessage("error", "Falha na conexao", error.message);
    } finally {
      setTestingSource(false);
    }
  };

  const handleUpdateSource = async (payload) => {
    setRegistering(true);
    try {
      const data = await updateDataSource({
        data_source_id: payload.id,
        source_type: payload.source_type,
        conn_secret_ref: payload.conn_secret_ref,
        rag_enabled: payload.rag_enabled,
        rag_context_text: payload.rag_context_text,
      });
      setEditingSource(null);
      setIsModalOpen(false);
      onSystemMessage(
        "success",
        tUi("onboarding.ok.sourceUpdated.title", "Fonte atualizada"),
        `Fonte ${data.source.source_name || data.source.source_type} atualizada com sucesso.`
      );
      await loadTenantSources();
    } catch (error) {
      onSystemMessage("error", tUi("onboarding.fail.sourceUpdate", "Falha na edicao da fonte"), translateSourceLimitMessage(error.message));
    } finally {
      setRegistering(false);
    }
  };

  const downloadTextFile = (filename, content, mimeType = "text/plain;charset=utf-8") => {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  };

  const handleExportSourceRag = async (source) => {
    if (!source?.id || !source?.conn_secret_ref) return;
    setExportingSourceId(source.id);
    try {
      const discovered = await discoverDataSourceTables({
        source_type: source.source_type,
        conn_secret_ref: source.conn_secret_ref,
      });
      const tables = Array.isArray(discovered?.tables) ? discovered.tables : [];
      const lines = [];
      lines.push("# Modelo RAG IAOps (texto amigavel)");
      lines.push(`# Fonte: ${source.source_name || source.source_type} (#${source.id})`);
      lines.push(`Gerado em: ${new Date().toISOString()}`);
      lines.push("");
      for (const table of tables) {
        const schemaName = String(table.schema_name || "public").trim();
        const tableName = String(table.table_name || "").trim();
        if (!tableName) continue;
        lines.push(`## Tabela: ${schemaName}.${tableName}`);
        lines.push(`Nome amigavel: ${tableName}`);
        lines.push("Descricao: -");
        lines.push("");
        lines.push("### Campos");
        lines.push("| Campo tecnico | Nome amigavel | Tipo | Sinonimos |");
        lines.push("| --- | --- | --- | --- |");
        try {
          const colsResp = await discoverDataSourceColumns({
            source_type: source.source_type,
            conn_secret_ref: source.conn_secret_ref,
            schema_name: schemaName,
            table_name: tableName,
          });
          const cols = Array.isArray(colsResp?.columns) ? colsResp.columns : [];
          for (const col of cols) {
            const colName = String(col.column_name || "").trim();
            const dataType = String(col.data_type || "").trim() || "unknown";
            if (!colName) continue;
            lines.push(`| ${colName} | ${colName} | ${dataType} | - |`);
          }
        } catch (_) {
          lines.push("| erro_ao_carregar_colunas | erro_ao_carregar_colunas | unknown | - |");
        }
        lines.push("");
        lines.push("### Relacionamentos");
        lines.push("| Tabela destino | Condicao de join | Descricao |");
        lines.push("| --- | --- | --- |");
        lines.push("| - | - | - |");
        lines.push("");
      }
      const safeSource = String(source.source_name || source.source_type || "fonte")
        .toLowerCase()
        .replace(/[^a-z0-9_-]+/g, "_");
      downloadTextFile(`modelo_rag_${safeSource}_${source.id}.md`, lines.join("\n"), "text/markdown;charset=utf-8");
      onSystemMessage("success", "Exportacao concluida", `Modelo RAG textual gerado com ${tables.length} tabela(s).`);
    } catch (error) {
      onSystemMessage("error", "Falha na exportacao RAG", error.message);
    } finally {
      setExportingSourceId(null);
    }
  };

  const handleRegisterMonitoredTable = async (payload) => {
    setTableSaving(true);
    try {
      const data = await registerMonitoredTable(payload);
      setIsTableModalOpen(false);
      onSystemMessage(
        "success",
        tUi("onboarding.ok.tableCreated.title", "Tabela monitorada cadastrada"),
        `Tabela ${data.table.schema_name}.${data.table.table_name} cadastrada com sucesso.`
      );
      await loadMonitoredTables(String(payload.data_source_id));
    } catch (error) {
      onSystemMessage("error", tUi("onboarding.fail.tableCreate", "Falha no cadastro da tabela monitorada"), error.message);
    } finally {
      setTableSaving(false);
    }
  };

  const openStatusAction = (source, nextIsActive) => {
    setPendingAction({
      type: "status",
      source,
      nextIsActive,
      title: nextIsActive ? tUi("onboarding.source.activate", "Ativar fonte") : tUi("onboarding.source.deactivate", "Inativar fonte"),
      message: `${nextIsActive ? tUi("onboarding.action.activate", "Ativar") : tUi("onboarding.action.deactivate", "Inativar")} a fonte ${
        source.source_name || source.source_type
      }?`,
      confirmLabel: nextIsActive ? tUi("onboarding.action.activate", "Ativar") : tUi("onboarding.action.deactivate", "Inativar"),
    });
  };

  const openDeleteAction = (source) => {
    setPendingAction({
      type: "delete",
      source,
      title: tUi("onboarding.source.remove", "Remover fonte"),
      message: `Remover a fonte ${source.source_name || source.source_type}? Essa acao nao pode ser desfeita.`,
      confirmLabel: tUi("common.remove", "Remover"),
    });
  };

  const handleConfirmAction = async () => {
    if (!pendingAction) return;
    setActionLoading(true);
    try {
      if (pendingAction.type === "status") {
        const data = await updateDataSourceStatus({
          data_source_id: pendingAction.source.id,
          is_active: pendingAction.nextIsActive,
        });
        setPendingAction(null);
        onSystemMessage(
          "success",
          tUi("onboarding.ok.sourceUpdated.title", "Fonte atualizada"),
          `Fonte ${data.source.source_name || data.source.source_type} atualizada para ${
            data.source.is_active ? tUi("common.active", "ativa") : tUi("common.inactive", "inativa")
          }.`
        );
      } else if (pendingAction.type === "delete") {
        const data = await deleteDataSource({ data_source_id: pendingAction.source.id });
        setPendingAction(null);
        onSystemMessage("success", tUi("onboarding.ok.sourceRemoved.title", "Fonte removida"), `Fonte ${data.result.source_type} removida com sucesso.`);
      } else if (pendingAction.type === "delete_table") {
        const data = await deleteMonitoredTable({ monitored_table_id: pendingAction.table.id });
        setPendingAction(null);
        onSystemMessage(
          "success",
          tUi("onboarding.ok.tableRemoved.title", "Tabela monitorada removida"),
          `Tabela ${data.result.schema_name}.${data.result.table_name} removida com sucesso.`
        );
      }
      await loadTenantSources();
      await loadMonitoredTables();
    } catch (error) {
      setPendingAction(null);
      onSystemMessage("error", tUi("onboarding.fail.action", "Falha na operacao"), translateSourceLimitMessage(error.message));
    } finally {
      setActionLoading(false);
    }
  };

  useEffect(() => {
    loadCatalog();
    loadTenantSources();
  }, []);

  useEffect(() => {
    if (selectedSourceIdForTables) {
      loadMonitoredTables(selectedSourceIdForTables);
    }
  }, [selectedSourceIdForTables]);

  const reachedClientLimit = limits
    ? Number(limits.total_data_sources || 0) >= Number(limits.max_data_sources_per_client || 0)
    : false;
  const reachedTenantLimit = limits
    ? Number(limits.total_data_sources_tenant || 0) >= Number(limits.max_data_sources_per_tenant || 0)
    : false;
  const sourceCreateBlocked = reachedClientLimit || reachedTenantLimit;
  const sourceCreateBlockedReason = reachedClientLimit
    ? `${tUi("onboarding.limit.sourceClient", "Limite de fontes ativas por cliente atingido no plano atual.")} (${Number(
        limits?.total_data_sources || 0
      )}/${Number(limits?.max_data_sources_per_client || 0)})`
    : reachedTenantLimit
      ? `${tUi("onboarding.limit.sourceTenant", "Limite de fontes ativas por tenant atingido no plano atual.")} (${Number(
          limits?.total_data_sources_tenant || 0
        )}/${Number(limits?.max_data_sources_per_tenant || 0)})`
      : "";

  return (
    <section className="page-panel">
      <header>
        <h2>{tUi("onboarding.header.title", "Onboarding")}</h2>
        <p>{tUi("onboarding.header.subtitle", "Catalogo de fontes suportadas para governanca: bancos, NoSQL, warehouses, lakes, Power BI e Fabric.")}</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-secondary" onClick={loadCatalog}>
          {tUi("onboarding.refresh.catalog", "Atualizar Catalogo")}
        </button>
        <button
          type="button"
          className="btn btn-primary"
          disabled={sourceCreateBlocked}
          title={sourceCreateBlocked ? sourceCreateBlockedReason : ""}
          onClick={() => {
            setEditingSource(null);
            setIsModalOpen(true);
          }}
        >
          {tUi("onboarding.create.source", "Cadastrar Fonte")}
        </button>
        <button type="button" className="btn btn-secondary" onClick={loadTenantSources}>
          {tUi("onboarding.refresh.tenantSources", "Atualizar Fontes do Tenant")}
        </button>
      </div>
      {limits ? (
        <div className="chip-row" style={{ marginBottom: "0.75rem" }}>
          <span className={`chip ${reachedClientLimit ? "chip-error" : "chip-muted"}`}>
            {`Fontes por cliente: ${Number(limits.total_data_sources || 0)}/${Number(limits.max_data_sources_per_client || 0)}`}
          </span>
          <span className={`chip ${reachedTenantLimit ? "chip-error" : "chip-muted"}`}>
            {`Fontes por tenant: ${Number(limits.total_data_sources_tenant || 0)}/${Number(limits.max_data_sources_per_tenant || 0)}`}
          </span>
        </div>
      ) : null}

      {loading && <p className="empty-state">{tUi("onboarding.loading.sources", "Carregando fontes...")}</p>}

      <section className="catalog-block">
        <h3>{tUi("onboarding.sources.title", "Fontes cadastradas no tenant")}</h3>
        {tenantSources.length === 0 ? (
          <p className="empty-state">{tUi("onboarding.sources.empty", "Nenhuma fonte cadastrada ainda.")}</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Fonte</th>
                  <th>Tipo</th>
                  <th>RAG</th>
                  <th>Database/Schema</th>
                  <th>Status</th>
                  <th>Acoes</th>
                </tr>
              </thead>
              <tbody>
                {tenantSources.map((item) => (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.source_name || item.source_type}</td>
                    <td>{item.source_type}</td>
                    <td>{item.rag_enabled ? "Ativo" : "Inativo"}</td>
                    <td>{parseSourceLocation(item.conn_secret_ref)}</td>
                    <td>{item.is_active ? tUi("common.active", "Ativa") : tUi("common.inactive", "Inativa")}</td>
                    <td>
                      <div className="chip-row">
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() => {
                            setEditingSource(item);
                            setIsModalOpen(true);
                          }}
                        >
                          {tUi("common.edit", "Editar")}
                        </button>
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() => openStatusAction(item, !item.is_active)}
                        >
                          {item.is_active ? tUi("onboarding.action.deactivate", "Inativar") : tUi("onboarding.action.activate", "Ativar")}
                        </button>
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() => handleExportSourceRag(item)}
                          disabled={exportingSourceId === item.id}
                        >
                          {exportingSourceId === item.id ? "Exportando..." : "Exportar RAG"}
                        </button>
                        <button type="button" className="btn btn-small btn-secondary" onClick={() => openDeleteAction(item)}>
                          {tUi("common.remove", "Remover")}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="catalog-block">
        <div className="section-header">
          <h3>{tUi("onboarding.tables.title", "Tabelas monitoradas por fonte")}</h3>
          <div className="chip-row">
            <button type="button" className="btn btn-primary btn-small" onClick={() => setIsTableModalOpen(true)}>
              {tUi("onboarding.create.table", "Cadastrar Tabela")}
            </button>
            <button type="button" className="btn btn-secondary btn-small" onClick={() => loadMonitoredTables(undefined, { forceSync: true })}>
              {tUi("common.refresh", "Atualizar")}
            </button>
          </div>
        </div>
        <div className="inline-form">
          <select
            value={selectedSourceIdForTables}
            onChange={(e) => setSelectedSourceIdForTables(e.target.value)}
            disabled={tenantSources.length === 0}
          >
            {tenantSources.map((item) => (
              <option key={item.id} value={item.id}>
                {`${(() => {
                  const location = parseSourceLocation(item.conn_secret_ref);
                  return location !== "-" ? location : item.source_name || item.source_type;
                })()} (#${item.id})`}
              </option>
            ))}
          </select>
        </div>
        {tablesLoading ? (
          <p className="empty-state">{tUi("onboarding.loading.tables", "Carregando tabelas monitoradas...")}</p>
        ) : monitoredTables.length === 0 ? (
          <p className="empty-state">{tUi("onboarding.tables.empty", "Nenhuma tabela monitorada para a fonte selecionada.")}</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Schema</th>
                  <th>Tabela</th>
                  <th>Status</th>
                  <th>Acoes</th>
                </tr>
              </thead>
              <tbody>
                {monitoredTables.map((item) => (
                  <tr key={item.id}>
                    <td>{item.schema_name}</td>
                    <td>{item.table_name}</td>
                    <td>{item.is_active ? tUi("common.active", "Ativa") : tUi("common.inactive", "Inativa")}</td>
                    <td>
                      <div className="chip-row">
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() =>
                            setPendingAction({
                              type: "delete_table",
                              table: item,
                              title: tUi("onboarding.table.remove", "Remover tabela monitorada"),
                              message: `Remover ${item.schema_name}.${item.table_name}?`,
                              confirmLabel: tUi("common.remove", "Remover"),
                            })
                          }
                        >
                          {tUi("common.remove", "Remover")}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <DataSourceFormModal
        open={isModalOpen}
        sourceCatalog={sources}
        initialData={editingSource}
        title={
          editingSource
            ? tUi("onboarding.modal.source.edit", "Editar fonte de dados do tenant")
            : tUi("onboarding.modal.source.new", "Nova fonte de dados do tenant")
        }
        submitLabel={editingSource ? tUi("common.saveChanges", "Salvar alteracoes") : tUi("common.create", "Cadastrar")}
        testing={testingSource}
        onTest={handleTestSourceConnection}
        onClose={() => {
          if (!registering) {
            setEditingSource(null);
            setIsModalOpen(false);
          }
        }}
        onSubmit={editingSource ? handleUpdateSource : handleRegisterSource}
      />

      <ConfirmActionModal
        open={Boolean(pendingAction)}
        title={pendingAction?.title || tUi("common.confirmation", "Confirmacao")}
        message={pendingAction?.message || ""}
        confirmLabel={pendingAction?.confirmLabel || tUi("common.confirm", "Confirmar")}
        loading={actionLoading}
        onConfirm={handleConfirmAction}
        onClose={() => {
          if (!actionLoading) setPendingAction(null);
        }}
      />

      <MonitoredTableFormModal
        open={isTableModalOpen}
        sources={tenantSources}
        defaultSourceId={selectedSourceIdForTables ? Number(selectedSourceIdForTables) : undefined}
        onClose={() => {
          if (!tableSaving) setIsTableModalOpen(false);
        }}
        onSubmit={handleRegisterMonitoredTable}
      />
    </section>
  );
}

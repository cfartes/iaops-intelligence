import { useEffect, useState } from "react";
import {
  deleteDataSource,
  discoverDataSourceColumns,
  discoverDataSourceTables,
  deleteMonitoredColumn,
  deleteMonitoredTable,
  enrichMonitoredColumns,
  listOnboardingMonitoredColumns,
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
  const [monitoredColumns, setMonitoredColumns] = useState([]);
  const [selectedTableIdForColumns, setSelectedTableIdForColumns] = useState("");
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [discoveringColumns, setDiscoveringColumns] = useState(false);
  const [pendingAction, setPendingAction] = useState(null);
  const [actionLoading, setActionLoading] = useState(false);

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
      const data = await listTenantDataSources();
      const rows = data.sources || [];
      setTenantSources(rows);
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
            if (refreshedRows.length > 0) {
              const tableExists = refreshedRows.some((item) => String(item.id) === String(selectedTableIdForColumns));
              if (!selectedTableIdForColumns || !tableExists) {
                setSelectedTableIdForColumns(String(refreshedRows[0].id));
              }
            }
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
      if (rows.length === 0) {
        setSelectedTableIdForColumns("");
        setMonitoredColumns([]);
        return;
      }
      const tableExists = rows.some((item) => String(item.id) === String(selectedTableIdForColumns));
      if (!selectedTableIdForColumns || !tableExists) {
        setSelectedTableIdForColumns(String(rows[0].id));
      }
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

  const loadMonitoredColumns = async (tableIdRaw, options = {}) => {
    const { attemptSync = true, forceSync = false } = options;
    const tableId = tableIdRaw || selectedTableIdForColumns || undefined;
    if (!tableId) {
      setMonitoredColumns([]);
      return;
    }
    setColumnsLoading(true);
    try {
      const data = await listOnboardingMonitoredColumns(Number(tableId));
      let rows = data.columns || [];
      const table = monitoredTables.find((item) => String(item.id) === String(tableId));
      if (table && (forceSync || (attemptSync && rows.length === 0))) {
        setDiscoveringColumns(true);
        try {
          await syncColumnsForTable(table);
          const refreshed = await listOnboardingMonitoredColumns(Number(tableId));
          rows = refreshed.columns || [];
        } catch (error) {
          onSystemMessage("warning", "Colunas nao detectadas automaticamente", error.message);
        } finally {
          setDiscoveringColumns(false);
        }
      }
      setMonitoredColumns(rows);
    } catch (error) {
      onSystemMessage("error", tUi("onboarding.fail.columns", "Erro ao carregar colunas monitoradas"), error.message);
    } finally {
      setColumnsLoading(false);
    }
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
      onSystemMessage("error", tUi("onboarding.fail.sourceCreate", "Falha no cadastro da fonte"), error.message);
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
      onSystemMessage("error", tUi("onboarding.fail.sourceUpdate", "Falha na edicao da fonte"), error.message);
    } finally {
      setRegistering(false);
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
        onSystemMessage(
          "success",
          tUi("onboarding.ok.sourceUpdated.title", "Fonte atualizada"),
          `Fonte ${data.source.source_name || data.source.source_type} atualizada para ${
            data.source.is_active ? tUi("common.active", "ativa") : tUi("common.inactive", "inativa")
          }.`
        );
      } else if (pendingAction.type === "delete") {
        const data = await deleteDataSource({ data_source_id: pendingAction.source.id });
        onSystemMessage("success", tUi("onboarding.ok.sourceRemoved.title", "Fonte removida"), `Fonte ${data.result.source_type} removida com sucesso.`);
      } else if (pendingAction.type === "delete_table") {
        const data = await deleteMonitoredTable({ monitored_table_id: pendingAction.table.id });
        onSystemMessage(
          "success",
          tUi("onboarding.ok.tableRemoved.title", "Tabela monitorada removida"),
          `Tabela ${data.result.schema_name}.${data.result.table_name} removida com sucesso.`
        );
      } else if (pendingAction.type === "delete_column") {
        const data = await deleteMonitoredColumn({ monitored_column_id: pendingAction.column.id });
        onSystemMessage(
          "success",
          tUi("onboarding.ok.columnRemoved.title", "Coluna monitorada removida"),
          `Coluna ${data.result.column_name} removida com sucesso.`
        );
      }
      setPendingAction(null);
      await loadTenantSources();
      await loadMonitoredTables();
      await loadMonitoredColumns();
    } catch (error) {
      onSystemMessage("error", tUi("onboarding.fail.action", "Falha na operacao"), error.message);
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

  useEffect(() => {
    if (selectedTableIdForColumns) {
      loadMonitoredColumns(selectedTableIdForColumns);
    }
  }, [selectedTableIdForColumns]);

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
                  <th>Secret Ref</th>
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
                    <td>{item.conn_secret_ref}</td>
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
                {(item.source_name || item.source_type) + ` (#${item.id})`}
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
                          onClick={() => setSelectedTableIdForColumns(String(item.id))}
                        >
                          {tUi("onboarding.columns.button", "Colunas")}
                        </button>
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

      <section className="catalog-block">
        <div className="section-header">
          <h3>{tUi("onboarding.columns.title", "Colunas monitoradas por tabela")}</h3>
          <div className="chip-row">
            <button type="button" className="btn btn-secondary btn-small" onClick={() => loadMonitoredColumns(undefined, { forceSync: true })}>
              {discoveringColumns ? "Sincronizando..." : "Sincronizar colunas"}
            </button>
          </div>
        </div>
        <div className="inline-form">
          <select
            value={selectedTableIdForColumns}
            onChange={(e) => setSelectedTableIdForColumns(e.target.value)}
            disabled={monitoredTables.length === 0}
          >
            {monitoredTables.map((item) => (
              <option key={item.id} value={item.id}>
                {`${item.schema_name}.${item.table_name} (#${item.id})`}
              </option>
            ))}
          </select>
        </div>
        {columnsLoading ? (
          <p className="empty-state">{tUi("onboarding.loading.columns", "Carregando colunas monitoradas...")}</p>
        ) : monitoredColumns.length === 0 ? (
          <p className="empty-state">{tUi("onboarding.columns.empty", "Nenhuma coluna monitorada para a tabela selecionada.")}</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Coluna</th>
                  <th>Tipo</th>
                  <th>Classificacao</th>
                  <th>Descricao</th>
                  <th>Acoes</th>
                </tr>
              </thead>
              <tbody>
                {monitoredColumns.map((item) => (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.column_name}</td>
                    <td>{item.data_type || "-"}</td>
                    <td>{item.classification || "-"}</td>
                    <td>{item.description_text || "-"}</td>
                    <td>
                      <button
                        type="button"
                        className="btn btn-small btn-secondary"
                        onClick={() =>
                          setPendingAction({
                            type: "delete_column",
                            column: item,
                            title: tUi("onboarding.column.remove", "Remover coluna monitorada"),
                            message: `Remover coluna ${item.column_name}?`,
                            confirmLabel: tUi("common.remove", "Remover"),
                          })
                        }
                      >
                        {tUi("common.remove", "Remover")}
                      </button>
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

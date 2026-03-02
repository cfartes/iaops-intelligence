import { useEffect, useMemo, useState } from "react";
import {
  deleteDataSource,
  deleteMonitoredColumn,
  deleteMonitoredTable,
  listOnboardingMonitoredColumns,
  listOnboardingMonitoredTables,
  listSourceCatalog,
  listTenantDataSources,
  registerDataSource,
  registerMonitoredColumn,
  registerMonitoredTable,
  updateDataSource,
  updateDataSourceStatus,
} from "../api/mcpApi";
import ConfirmActionModal from "../components/ConfirmActionModal";
import DataSourceFormModal from "../components/DataSourceFormModal";
import MonitoredColumnFormModal from "../components/MonitoredColumnFormModal";
import MonitoredTableFormModal from "../components/MonitoredTableFormModal";
import { tUi } from "../i18n/uiText";

const CATEGORY_LABEL = {
  relational: "Relacionais",
  nosql: "NoSQL",
  warehouse: "Data Warehouses",
  lake_storage: "Data Lakes/Object Storage",
  bi_semantic: "BI Semantico",
  lakehouse_semantic: "Lakehouse/Semantico",
};

export default function OnboardingPanel({ onSystemMessage }) {
  const [sources, setSources] = useState([]);
  const [tenantSources, setTenantSources] = useState([]);
  const [loading, setLoading] = useState(false);
  const [registering, setRegistering] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingSource, setEditingSource] = useState(null);
  const [monitoredTables, setMonitoredTables] = useState([]);
  const [selectedSourceIdForTables, setSelectedSourceIdForTables] = useState("");
  const [tablesLoading, setTablesLoading] = useState(false);
  const [isTableModalOpen, setIsTableModalOpen] = useState(false);
  const [tableSaving, setTableSaving] = useState(false);
  const [monitoredColumns, setMonitoredColumns] = useState([]);
  const [selectedTableIdForColumns, setSelectedTableIdForColumns] = useState("");
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [isColumnModalOpen, setIsColumnModalOpen] = useState(false);
  const [columnSaving, setColumnSaving] = useState(false);
  const [pendingAction, setPendingAction] = useState(null);
  const [actionLoading, setActionLoading] = useState(false);

  const grouped = useMemo(() => {
    return sources.reduce((acc, item) => {
      const key = item.category || "outros";
      if (!acc[key]) acc[key] = [];
      acc[key].push(item);
      return acc;
    }, {});
  }, [sources]);

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

  const loadMonitoredTables = async (sourceIdRaw) => {
    const sourceId = sourceIdRaw || selectedSourceIdForTables || undefined;
    setTablesLoading(true);
    try {
      const data = await listOnboardingMonitoredTables(sourceId ? Number(sourceId) : undefined);
      const rows = data.tables || [];
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

  const loadMonitoredColumns = async (tableIdRaw) => {
    const tableId = tableIdRaw || selectedTableIdForColumns || undefined;
    if (!tableId) {
      setMonitoredColumns([]);
      return;
    }
    setColumnsLoading(true);
    try {
      const data = await listOnboardingMonitoredColumns(Number(tableId));
      setMonitoredColumns(data.columns || []);
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

  const handleRegisterMonitoredColumn = async (payload) => {
    setColumnSaving(true);
    try {
      const data = await registerMonitoredColumn(payload);
      setIsColumnModalOpen(false);
      onSystemMessage(
        "success",
        tUi("onboarding.ok.columnCreated.title", "Coluna monitorada cadastrada"),
        `Coluna ${data.column.column_name} cadastrada com sucesso.`
      );
      await loadMonitoredColumns(String(payload.monitored_table_id));
    } catch (error) {
      onSystemMessage("error", tUi("onboarding.fail.columnCreate", "Falha no cadastro da coluna monitorada"), error.message);
    } finally {
      setColumnSaving(false);
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

      {!loading &&
        Object.entries(grouped).map(([category, items]) => (
          <section key={category} className="catalog-block">
            <h3>{CATEGORY_LABEL[category] || category}</h3>
            <div className="chip-row">
              {items.map((item) => (
                <span key={item.code} className="chip">
                  {item.name}
                </span>
              ))}
            </div>
          </section>
        ))}

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
            <button type="button" className="btn btn-secondary btn-small" onClick={() => loadMonitoredTables()}>
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
                  <th>ID</th>
                  <th>Fonte</th>
                  <th>Schema</th>
                  <th>Tabela</th>
                  <th>Status</th>
                  <th>Acoes</th>
                </tr>
              </thead>
              <tbody>
                {monitoredTables.map((item) => (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.source_name || item.source_type}</td>
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
            <button type="button" className="btn btn-primary btn-small" onClick={() => setIsColumnModalOpen(true)}>
              {tUi("onboarding.create.column", "Cadastrar Coluna")}
            </button>
            <button type="button" className="btn btn-secondary btn-small" onClick={() => loadMonitoredColumns()}>
              {tUi("common.refresh", "Atualizar")}
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

      <MonitoredColumnFormModal
        open={isColumnModalOpen}
        tables={monitoredTables}
        defaultTableId={selectedTableIdForColumns ? Number(selectedTableIdForColumns) : undefined}
        onClose={() => {
          if (!columnSaving) setIsColumnModalOpen(false);
        }}
        onSubmit={handleRegisterMonitoredColumn}
      />
    </section>
  );
}

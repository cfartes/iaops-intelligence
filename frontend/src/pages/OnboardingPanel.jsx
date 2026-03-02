import { useEffect, useMemo, useState } from "react";
import {
  deleteDataSource,
  deleteMonitoredTable,
  listSourceCatalog,
  listOnboardingMonitoredTables,
  listTenantDataSources,
  registerDataSource,
  registerMonitoredTable,
  updateDataSource,
  updateDataSourceStatus,
} from "../api/mcpApi";
import DataSourceFormModal from "../components/DataSourceFormModal";
import ConfirmActionModal from "../components/ConfirmActionModal";
import MonitoredTableFormModal from "../components/MonitoredTableFormModal";

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
      onSystemMessage("error", "Erro no onboarding", error.message);
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
      onSystemMessage("error", "Erro ao carregar fontes do tenant", error.message);
    }
  };

  const loadMonitoredTables = async (sourceIdRaw) => {
    const sourceId = sourceIdRaw || selectedSourceIdForTables || undefined;
    setTablesLoading(true);
    try {
      const data = await listOnboardingMonitoredTables(sourceId ? Number(sourceId) : undefined);
      setMonitoredTables(data.tables || []);
    } catch (error) {
      onSystemMessage("error", "Erro ao carregar tabelas monitoradas", error.message);
    } finally {
      setTablesLoading(false);
    }
  };

  const handleRegisterSource = async (payload) => {
    setRegistering(true);
    try {
      const data = await registerDataSource(payload);
      setIsModalOpen(false);
      onSystemMessage(
        "success",
        "Fonte cadastrada",
        `Fonte ${data.source.source_name || data.source.source_type} cadastrada com sucesso.`
      );
      await loadTenantSources();
    } catch (error) {
      onSystemMessage("error", "Falha no cadastro da fonte", error.message);
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
        "Fonte atualizada",
        `Fonte ${data.source.source_name || data.source.source_type} atualizada com sucesso.`
      );
      await loadTenantSources();
    } catch (error) {
      onSystemMessage("error", "Falha na edicao da fonte", error.message);
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
        "Tabela monitorada cadastrada",
        `Tabela ${data.table.schema_name}.${data.table.table_name} cadastrada com sucesso.`
      );
      await loadMonitoredTables(String(payload.data_source_id));
    } catch (error) {
      onSystemMessage("error", "Falha no cadastro da tabela monitorada", error.message);
    } finally {
      setTableSaving(false);
    }
  };

  const openStatusAction = (source, nextIsActive) => {
    setPendingAction({
      type: "status",
      source,
      nextIsActive,
      title: nextIsActive ? "Ativar fonte" : "Inativar fonte",
      message: `${nextIsActive ? "Ativar" : "Inativar"} a fonte ${source.source_name || source.source_type}?`,
      confirmLabel: nextIsActive ? "Ativar" : "Inativar",
    });
  };

  const openDeleteAction = (source) => {
    setPendingAction({
      type: "delete",
      source,
      title: "Remover fonte",
      message: `Remover a fonte ${source.source_name || source.source_type}? Essa acao nao pode ser desfeita.`,
      confirmLabel: "Remover",
    });
  };

  const handleConfirmAction = async () => {
    if (!pendingAction?.source?.id) return;
    setActionLoading(true);
    try {
      if (pendingAction.type === "status") {
        const data = await updateDataSourceStatus({
          data_source_id: pendingAction.source.id,
          is_active: pendingAction.nextIsActive,
        });
        onSystemMessage(
          "success",
          "Fonte atualizada",
          `Fonte ${data.source.source_name || data.source.source_type} atualizada para ${
            data.source.is_active ? "ativa" : "inativa"
          }.`
        );
      } else if (pendingAction.type === "delete") {
        const data = await deleteDataSource({ data_source_id: pendingAction.source.id });
        onSystemMessage("success", "Fonte removida", `Fonte ${data.result.source_type} removida com sucesso.`);
      } else if (pendingAction.type === "delete_table") {
        const data = await deleteMonitoredTable({ monitored_table_id: pendingAction.table.id });
        onSystemMessage(
          "success",
          "Tabela monitorada removida",
          `Tabela ${data.result.schema_name}.${data.result.table_name} removida com sucesso.`
        );
      }
      setPendingAction(null);
      await loadTenantSources();
      await loadMonitoredTables();
    } catch (error) {
      onSystemMessage("error", "Falha na operacao", error.message);
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

  return (
    <section className="page-panel">
      <header>
        <h2>Onboarding</h2>
        <p>Catalogo de fontes suportadas para governanca: bancos, NoSQL, warehouses, lakes, Power BI e Fabric.</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-secondary" onClick={loadCatalog}>
          Atualizar Catalogo
        </button>
        <button
          type="button"
          className="btn btn-primary"
          onClick={() => {
            setEditingSource(null);
            setIsModalOpen(true);
          }}
        >
          Cadastrar Fonte
        </button>
        <button type="button" className="btn btn-secondary" onClick={loadTenantSources}>
          Atualizar Fontes do Tenant
        </button>
      </div>

      {loading && <p className="empty-state">Carregando fontes...</p>}

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
        <h3>Fontes cadastradas no tenant</h3>
        {tenantSources.length === 0 ? (
          <p className="empty-state">Nenhuma fonte cadastrada ainda.</p>
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
                    <td>{item.is_active ? "Ativa" : "Inativa"}</td>
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
                          Editar
                        </button>
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() => openStatusAction(item, !item.is_active)}
                        >
                          {item.is_active ? "Inativar" : "Ativar"}
                        </button>
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() => openDeleteAction(item)}
                        >
                          Remover
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
          <h3>Tabelas monitoradas por fonte</h3>
          <div className="chip-row">
            <button type="button" className="btn btn-primary btn-small" onClick={() => setIsTableModalOpen(true)}>
              Cadastrar Tabela
            </button>
            <button type="button" className="btn btn-secondary btn-small" onClick={() => loadMonitoredTables()}>
              Atualizar
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
          <p className="empty-state">Carregando tabelas monitoradas...</p>
        ) : monitoredTables.length === 0 ? (
          <p className="empty-state">Nenhuma tabela monitorada para a fonte selecionada.</p>
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
                    <td>{item.is_active ? "Ativa" : "Inativa"}</td>
                    <td>
                      <button
                        type="button"
                        className="btn btn-small btn-secondary"
                        onClick={() =>
                          setPendingAction({
                            type: "delete_table",
                            table: item,
                            title: "Remover tabela monitorada",
                            message: `Remover ${item.schema_name}.${item.table_name}?`,
                            confirmLabel: "Remover",
                          })
                        }
                      >
                        Remover
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
        title={editingSource ? "Editar fonte de dados do tenant" : "Nova fonte de dados do tenant"}
        submitLabel={editingSource ? "Salvar alteracoes" : "Cadastrar"}
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
        title={pendingAction?.title || "Confirmacao"}
        message={pendingAction?.message || ""}
        confirmLabel={pendingAction?.confirmLabel || "Confirmar"}
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

import { useState } from "react";
import { listColumns, listTables } from "../api/mcpApi";

export default function InventoryPanel({ onSystemMessage }) {
  const [schemaName, setSchemaName] = useState("public");
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState("");
  const [columns, setColumns] = useState([]);

  const loadTables = async () => {
    try {
      const data = await listTables(schemaName || undefined);
      setTables(data.tables || []);
      setColumns([]);
      setSelectedTable("");
    } catch (error) {
      onSystemMessage("error", "Erro ao listar tabelas", error.message);
    }
  };

  const loadColumns = async () => {
    if (!selectedTable) {
      onSystemMessage("warning", "Selecione uma tabela", "Escolha uma tabela para listar colunas.");
      return;
    }
    try {
      const data = await listColumns(schemaName, selectedTable);
      setColumns(data.columns || []);
    } catch (error) {
      onSystemMessage("error", "Erro ao listar colunas", error.message);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>Inventario</h2>
        <p>Consulta de tabelas e colunas monitoradas via MCP.</p>
      </header>
      <div className="inline-form">
        <input value={schemaName} onChange={(e) => setSchemaName(e.target.value)} placeholder="Schema" />
        <button type="button" className="btn btn-primary" onClick={loadTables}>
          Carregar Tabelas
        </button>
      </div>

      <div className="list-grid">
        <div>
          <h3>Tabelas</h3>
          <ul className="data-list">
            {tables.map((item) => (
              <li key={`${item.schema_name}.${item.table_name}`}>
                <button
                  type="button"
                  className={selectedTable === item.table_name ? "list-item active" : "list-item"}
                  onClick={() => setSelectedTable(item.table_name)}
                >
                  {item.schema_name}.{item.table_name}
                </button>
              </li>
            ))}
          </ul>
        </div>

        <div>
          <div className="section-header">
            <h3>Colunas</h3>
            <button type="button" className="btn btn-secondary" onClick={loadColumns}>
              Carregar Colunas
            </button>
          </div>
          <ul className="data-list">
            {columns.map((item) => (
              <li key={item.column_name} className="column-item">
                <strong>{item.column_name}</strong>
                <span>{item.data_type || "n/a"}</span>
                <span>{item.classification || "n/a"}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  );
}
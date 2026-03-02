import { useMemo, useState } from "react";
import { runChatBiQuery } from "../api/mcpApi";

function suggestSqlFromQuestion(question) {
  const q = question.toLowerCase();
  if (q.includes("incidente") && q.includes("aberto")) {
    return "SELECT status, COUNT(*) AS total FROM iaops_gov.incident WHERE status IN ('open','ack') GROUP BY status";
  }
  if (q.includes("evento") && q.includes("critico")) {
    return "SELECT severity, COUNT(*) AS total FROM iaops_gov.schema_change_event WHERE severity = 'critical' GROUP BY severity";
  }
  if (q.includes("tabela") || q.includes("inventario")) {
    return "SELECT schema_name, table_name FROM iaops_gov.monitored_table ORDER BY schema_name, table_name LIMIT 50";
  }
  return "SELECT NOW() AS server_time";
}

function summarizeResult(rows) {
  if (!rows || rows.length === 0) return "Consulta executada com sucesso, sem linhas retornadas.";
  return `Consulta executada com sucesso. Foram retornadas ${rows.length} linha(s).`;
}

export default function ChatBiPanel({ onSystemMessage }) {
  const [question, setQuestion] = useState("");
  const [sqlText, setSqlText] = useState("SELECT NOW() AS server_time");
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);

  const columns = useMemo(() => result?.columns || [], [result]);
  const rows = useMemo(() => result?.rows || [], [result]);

  const generateSql = () => {
    if (!question.trim()) {
      onSystemMessage("warning", "Pergunta vazia", "Informe uma pergunta para gerar o SQL sugerido.");
      return;
    }
    setSqlText(suggestSqlFromQuestion(question));
  };

  const runQuery = async () => {
    if (!sqlText.trim()) {
      onSystemMessage("warning", "SQL vazio", "Informe um SQL para executar.");
      return;
    }
    setLoading(true);
    try {
      const data = await runChatBiQuery({ sql_text: sqlText, explain: false });
      setResult(data);
    } catch (error) {
      onSystemMessage("error", "Falha no Chat BI", error.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>Chat BI</h2>
        <p>Pergunta em linguagem natural com SQL assistido e execucao segura.</p>
      </header>

      <div className="form-grid chat-grid">
        <label>
          Pergunta
          <textarea
            className="chat-textarea"
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            placeholder="Ex.: Quantos incidentes abertos temos agora?"
          />
        </label>

        <div className="page-actions">
          <button type="button" className="btn btn-secondary" onClick={generateSql}>
            Sugerir SQL
          </button>
          <button type="button" className="btn btn-primary" onClick={runQuery}>
            Executar SQL Seguro
          </button>
        </div>

        <label>
          SQL gerado/editavel
          <textarea className="chat-textarea sql-area" value={sqlText} onChange={(e) => setSqlText(e.target.value)} />
        </label>
      </div>

      {loading && <p className="empty-state">Executando consulta...</p>}

      {result && !loading && (
        <div className="chat-result">
          <p className="chat-summary">{summarizeResult(rows)}</p>

          {columns.length > 0 && rows.length > 0 && (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    {columns.map((col) => (
                      <th key={col}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, idx) => (
                    <tr key={idx}>
                      {columns.map((col) => (
                        <td key={`${idx}-${col}`}>{String(row[col] ?? "")}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </section>
  );
}
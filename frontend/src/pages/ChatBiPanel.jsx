import { useMemo, useState } from "react";
import { runChatBiQuery } from "../api/mcpApi";

function summarizeResult(rows) {
  if (!rows || rows.length === 0) return "Pergunta processada com sucesso, sem registros retornados.";
  return `Pergunta processada com sucesso. Foram retornadas ${rows.length} linha(s).`;
}

export default function ChatBiPanel({ onSystemMessage }) {
  const [question, setQuestion] = useState("");
  const [answer, setAnswer] = useState("");
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);

  const resultData = useMemo(() => result?.result || {}, [result]);
  const columns = useMemo(() => resultData?.columns || [], [resultData]);
  const rows = useMemo(() => resultData?.rows || [], [resultData]);

  const askQuestion = async () => {
    if (!question.trim()) {
      onSystemMessage("warning", "Pergunta vazia", "Informe uma pergunta em linguagem natural.");
      return;
    }
    setLoading(true);
    setAnswer("");
    setResult(null);
    try {
      const data = await runChatBiQuery({ question_text: question.trim() });
      setResult(data);
      const preview = data?.result?.rows?.[0];
      if (preview && typeof preview === "object" && Object.prototype.hasOwnProperty.call(preview, "total")) {
        setAnswer(`Total encontrado: ${preview.total}.`);
      } else {
        setAnswer(`Consulta concluida com ${data?.result?.rows?.length || 0} linha(s).`);
      }
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
        <p>Perguntas em linguagem natural com RAG de metadados e execucao segura.</p>
      </header>

      <div className="form-grid chat-grid">
        <label>
          Pergunta
          <textarea
            className="chat-textarea"
            value={question}
            onChange={(event) => setQuestion(event.target.value)}
            placeholder="Ex.: Quantos incidentes abertos temos agora?"
          />
        </label>

        <div className="page-actions">
          <button type="button" className="btn btn-primary" onClick={askQuestion}>
            Perguntar
          </button>
        </div>
      </div>

      {loading && <p className="empty-state">Processando pergunta...</p>}

      {answer && !loading && <p className="chat-summary">{answer}</p>}

      {result && !loading && (
        <div className="chat-result">
          <p className="chat-summary">{summarizeResult(rows)}</p>
          <p className="muted">Consulta gerada internamente: {result.planned_sql}</p>

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

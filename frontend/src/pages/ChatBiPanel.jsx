import { useMemo, useState } from "react";
import { runChatBiQuery } from "../api/mcpApi";
import { tUi } from "../i18n/uiText";

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
  const responseMode = useMemo(() => result?.chat_response_mode || "executive", [result]);

  const askQuestion = async () => {
    if (!question.trim()) {
      onSystemMessage("warning", tUi("chat.emptyQuestion.title", "Pergunta vazia"), tUi("chat.emptyQuestion.message", "Informe uma pergunta em linguagem natural."));
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
      if (error?.code === "lgpd_blocked") {
        const blockedFields = error?.details?.blocked_fields || [];
        const preview = blockedFields.length > 0 ? blockedFields.slice(0, 5).join(", ") : "-";
        onSystemMessage(
          "warning",
          tUi("chat.lgpdBlocked.title", "Resposta bloqueada por LGPD"),
          `${tUi("chat.lgpdBlocked.message", "Essa pergunta toca em dados protegidos por politica LGPD deste tenant.")} ${tUi("chat.lgpdBlocked.fields", "Campos")} : ${preview}`,
        );
      } else if (error?.code === "tenant_blocked") {
        onSystemMessage(
          "warning",
          "Tenant bloqueado",
          "Tenant bloqueado por inadimplencia ou inatividade. Regularize o faturamento para continuar usando o Chat BI.",
        );
      } else {
        onSystemMessage("error", tUi("chat.fail.title", "Falha no Chat BI"), error.message);
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>Chat BI</h2>
        <p>{tUi("chat.header.subtitle", "Perguntas em linguagem natural com RAG de metadados e execucao segura.")}</p>
      </header>

      <div className="form-grid chat-grid">
        <label>
          {tUi("chat.label.question", "Pergunta")}
          <textarea
            className="chat-textarea"
            value={question}
            onChange={(event) => setQuestion(event.target.value)}
            placeholder={tUi("chat.placeholder.question", "Ex.: Quantos incidentes abertos temos agora?")}
          />
        </label>

        <div className="page-actions">
          <button type="button" className="btn btn-primary" onClick={askQuestion}>
            {tUi("chat.ask", "Perguntar")}
          </button>
        </div>
      </div>

      {loading && <p className="empty-state">{tUi("chat.loading", "Processando pergunta...")}</p>}

      {answer && !loading && <p className="chat-summary">{answer}</p>}

      {result && !loading && (
        <div className="chat-result">
          <p className="chat-summary">
            {summarizeResult(rows)} {tUi("chat.mode.label", "Modo")}: {responseMode === "detailed" ? tUi("chat.mode.detailed", "Detalhada") : tUi("chat.mode.executive", "Executiva")}.
          </p>

          {responseMode === "detailed" && columns.length > 0 && rows.length > 0 && (
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

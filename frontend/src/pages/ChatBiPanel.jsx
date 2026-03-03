import { useEffect, useMemo, useState } from "react";
import { listTenantDataSources, runChatBiQuery } from "../api/mcpApi";
import { tUi } from "../i18n/uiText";

function summarizeResult(rows) {
  if (!rows || rows.length === 0) return "Pergunta processada com sucesso, sem registros retornados.";
  return `Pergunta processada com sucesso. Foram retornadas ${rows.length} linha(s).`;
}

function SimpleBarChart({ visualization }) {
  const labels = Array.isArray(visualization?.labels) ? visualization.labels : [];
  const series = Array.isArray(visualization?.series) ? visualization.series : [];
  const values = Array.isArray(series?.[0]?.values) ? series[0].values : [];
  if (labels.length === 0 || values.length === 0) return null;
  const max = Math.max(...values.map((v) => Number(v) || 0), 0) || 1;
  return (
    <div className="metric-card" style={{ marginTop: 12 }}>
      <h4>{visualization?.title || "Grafico"}</h4>
      <div style={{ display: "grid", gap: 8 }}>
        {labels.slice(0, 12).map((label, idx) => {
          const value = Number(values[idx] || 0);
          const pct = Math.max(2, Math.round((value / max) * 100));
          return (
            <div key={`${label}-${idx}`} style={{ display: "grid", gridTemplateColumns: "180px 1fr 70px", gap: 8, alignItems: "center" }}>
              <span style={{ fontSize: 12, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{label}</span>
              <div style={{ height: 14, background: "#e7edf2", borderRadius: 999, overflow: "hidden" }}>
                <div style={{ width: `${pct}%`, height: "100%", background: "#0f5c84" }} />
              </div>
              <strong style={{ fontSize: 12, textAlign: "right" }}>{value}</strong>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default function ChatBiPanel({ onSystemMessage }) {
  const [question, setQuestion] = useState("");
  const [answer, setAnswer] = useState("");
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [sources, setSources] = useState([]);
  const [selectedSourceId, setSelectedSourceId] = useState("all");

  const resultData = useMemo(() => result?.result || {}, [result]);
  const columns = useMemo(() => resultData?.columns || [], [resultData]);
  const rows = useMemo(() => resultData?.rows || [], [resultData]);
  const responseMode = useMemo(() => result?.chat_response_mode || "executive", [result]);
  const visualization = useMemo(() => result?.visualization || null, [result]);
  const needsDimension = useMemo(() => Boolean(result?.intent?.needs_dimension), [result]);

  useEffect(() => {
    const loadSources = async () => {
      try {
        const data = await listTenantDataSources();
        const rows = Array.isArray(data?.sources) ? data.sources : [];
        setSources(rows);
      } catch (_) {
        setSources([]);
      }
    };
    loadSources();
  }, []);

  const askQuestion = async () => {
    if (!question.trim()) {
      onSystemMessage("warning", tUi("chat.emptyQuestion.title", "Pergunta vazia"), tUi("chat.emptyQuestion.message", "Informe uma pergunta em linguagem natural."));
      return;
    }
    setLoading(true);
    setAnswer("");
    setResult(null);
    try {
      const payload = { question_text: question.trim() };
      if (selectedSourceId !== "all") {
        payload.data_source_id = Number(selectedSourceId);
      }
      const data = await runChatBiQuery(payload);
      setResult(data);
      const natural = String(data?.reply_text || "").trim();
      if (natural) {
        setAnswer(natural);
      } else {
        const preview = data?.result?.rows?.[0];
        if (preview && typeof preview === "object" && Object.prototype.hasOwnProperty.call(preview, "total")) {
          setAnswer(`Total encontrado: ${preview.total}.`);
        } else {
          setAnswer(`Consulta concluida com ${data?.result?.rows?.length || 0} linha(s).`);
        }
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
          Fonte de dados (escopo)
          <select value={selectedSourceId} onChange={(event) => setSelectedSourceId(event.target.value)}>
            <option value="all">Todas as fontes</option>
            {sources.map((source) => (
              <option key={source.id} value={String(source.id)}>
                {(source.source_name || source.source_type) + ` (#${source.id})`}
              </option>
            ))}
          </select>
        </label>

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
          {responseMode === "detailed" && (
            <p className="chat-summary">
              {summarizeResult(rows)} {tUi("chat.mode.label", "Modo")}: {tUi("chat.mode.detailed", "Detalhada")}.
            </p>
          )}

          {visualization && <SimpleBarChart visualization={visualization} />}

          {(responseMode === "detailed" || needsDimension) && columns.length > 0 && rows.length > 0 && (
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

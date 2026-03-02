import { useEffect, useState } from "react";
import { channelWebhookTelegram, channelWebhookWhatsapp, getOperationHealth } from "../api/mcpApi";
import { tUi } from "../i18n/uiText";

export default function OperationPanel({ onSystemMessage }) {
  const [health, setHealth] = useState(null);
  const [channelType, setChannelType] = useState("telegram");
  const [externalUserKey, setExternalUserKey] = useState("tg-owner-demo");
  const [conversationKey, setConversationKey] = useState("chat-owner-demo");
  const [messageText, setMessageText] = useState("tenant list");
  const [webhookResponse, setWebhookResponse] = useState(null);
  const [isSending, setIsSending] = useState(false);

  const loadHealth = async () => {
    try {
      const data = await getOperationHealth(60);
      setHealth(data);
    } catch (error) {
      onSystemMessage("error", tUi("op.fail.health", "Erro ao carregar saude operacional"), error.message);
    }
  };

  useEffect(() => {
    loadHealth();
  }, []);

  useEffect(() => {
    if (channelType === "telegram") {
      setExternalUserKey("tg-owner-demo");
      setConversationKey("chat-owner-demo");
      return;
    }
    setExternalUserKey("wa-owner-demo");
    setConversationKey("wa-owner-demo");
  }, [channelType]);

  const sendChannelMessage = async () => {
    if (!externalUserKey.trim() || !conversationKey.trim()) {
      onSystemMessage("warning", tUi("op.required.title", "Campos obrigatorios"), tUi("op.required.message", "Informe external_user_key e conversation_key."));
      return;
    }
    setIsSending(true);
    setWebhookResponse(null);
    try {
      const payload = {
        external_user_key: externalUserKey.trim(),
        conversation_key: conversationKey.trim(),
        text: messageText.trim(),
      };
      const data =
        channelType === "telegram"
          ? await channelWebhookTelegram(payload)
          : await channelWebhookWhatsapp(payload);
      setWebhookResponse(data);
      onSystemMessage("success", tUi("op.webhook.ok.title", "Webhook processado"), tUi("op.webhook.ok.message", "Mensagem processada com sucesso no canal."));
    } catch (error) {
      onSystemMessage("error", tUi("op.webhook.fail.title", "Erro no webhook"), error.message);
    } finally {
      setIsSending(false);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>{tUi("op.header.title", "Operacao")}</h2>
        <p>{tUi("op.header.subtitle", "Painel de saude operacional e canais de notificacao.")}</p>
      </header>

      <div className="page-actions">
        <button type="button" className="btn btn-secondary" onClick={loadHealth}>
          {tUi("op.refresh", "Atualizar Saude")}
        </button>
      </div>

      {!health && <p className="empty-state">{tUi("op.empty", "Sem dados de saude.")}</p>}

      {health && (
        <div className="metric-grid">
          <article className="metric-card">
            <h4>{tUi("op.metric.openIncidents", "Incidentes abertos")}</h4>
            <strong>{health.open_incidents}</strong>
          </article>
          <article className="metric-card">
            <h4>{tUi("op.metric.criticalEvents", "Eventos criticos (janela)")}</h4>
            <strong>{health.critical_events}</strong>
          </article>
          <article className="metric-card">
            <h4>{tUi("op.metric.lastScan", "Ultima varredura")}</h4>
            <strong>{health.last_scan_at || "n/a"}</strong>
          </article>
          <article className="metric-card">
            <h4>{tUi("op.metric.channels", "Canais")}</h4>
            <div className="chip-row">
              {Object.entries(health.channels_health || {}).map(([name, status]) => (
                <span key={name} className="chip">{name}: {status}</span>
              ))}
            </div>
          </article>
        </div>
      )}

      <section className="catalog-block channel-tester">
        <h3>{tUi("op.tester.title", "Tester de Canal (Telegram/WhatsApp)")}</h3>
        <p className="muted">
          {tUi("op.tester.subtitle", "Simula entrada de webhook com comandos e linguagem natural.")}
        </p>

        <div className="inline-form">
          <select value={channelType} onChange={(event) => setChannelType(event.target.value)}>
            <option value="telegram">Telegram</option>
            <option value="whatsapp">WhatsApp</option>
          </select>
          <input
            value={externalUserKey}
            onChange={(event) => setExternalUserKey(event.target.value)}
            placeholder="external_user_key"
          />
          <input
            value={conversationKey}
            onChange={(event) => setConversationKey(event.target.value)}
            placeholder="conversation_key"
          />
        </div>

        <div className="inline-form">
          <input
            value={messageText}
            onChange={(event) => setMessageText(event.target.value)}
            placeholder={tUi("op.tester.message.placeholder", "Mensagem / comando")}
          />
          <button type="button" className="btn btn-primary" onClick={sendChannelMessage} disabled={isSending}>
            {isSending ? tUi("op.tester.sending", "Enviando...") : tUi("op.tester.send", "Enviar para Webhook")}
          </button>
        </div>

        {webhookResponse && (
          <article className="metric-card webhook-output">
            <h4>{tUi("op.tester.reply", "Resposta do Bot")}</h4>
            <pre>{webhookResponse.reply_text || tUi("op.tester.noReply", "Sem resposta textual")}</pre>
          </article>
        )}
      </section>
    </section>
  );
}

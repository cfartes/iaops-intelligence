import { useEffect, useState } from "react";
import {
  beginMfaSetup,
  disableMfa,
  enableMfa,
  getAdminLlmConfig,
  getAdminSmtpConfig,
  getMfaStatus,
  getUserTenantPreference,
  getTenantLlmConfig,
  getAuthContext,
  listAdminLlmModels,
  listAdminLlmProviders,
  listAuthSessions,
  listTenantLlmModels,
  listTenantLlmProviders,
  revokeAuthSession,
  updateUserTenantPreference,
  updateTenantLlmConfig,
  updateAdminLlmConfig,
  updateAdminSmtpConfig,
  sendAdminSmtpTestEmail,
  testAdminSmtpConfig,
} from "../api/mcpApi";
import AppLlmConfigModal from "../components/AppLlmConfigModal";
import ConfirmActionModal from "../components/ConfirmActionModal";
import MfaCodeModal from "../components/MfaCodeModal";
import SmtpConfigModal from "../components/SmtpConfigModal";
import TenantLlmConfigModal from "../components/TenantLlmConfigModal";
import { tUi } from "../i18n/uiText";

export default function ConfiguracaoPanel({ onSystemMessage, onPreferenceApplied, onSessionRevokedCurrent }) {
  const auth = getAuthContext();
  const isGlobalSuperadmin = Boolean(auth?.is_superadmin) && Number(auth?.tenant_id || 0) <= 0;
  const [mfa, setMfa] = useState(null);
  const [loading, setLoading] = useState(false);
  const [setupInfo, setSetupInfo] = useState(null);
  const [modalMode, setModalMode] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [llmProviders, setLlmProviders] = useState([]);
  const [llmModelsByProvider, setLlmModelsByProvider] = useState({});
  const [llmConfig, setLlmConfig] = useState(null);
  const [llmDenied, setLlmDenied] = useState(false);
  const [llmModalOpen, setLlmModalOpen] = useState(false);
  const [smtpConfig, setSmtpConfig] = useState(null);
  const [smtpModalOpen, setSmtpModalOpen] = useState(false);
  const [tenantLlmProviders, setTenantLlmProviders] = useState([]);
  const [tenantLlmModelsByProvider, setTenantLlmModelsByProvider] = useState({});
  const [tenantLlmConfig, setTenantLlmConfig] = useState(null);
  const [tenantLlmModalOpen, setTenantLlmModalOpen] = useState(false);
  const [userPref, setUserPref] = useState(null);
  const [languageDraft, setLanguageDraft] = useState("pt-BR");
  const [themeDraft, setThemeDraft] = useState("light");
  const [chatResponseModeDraft, setChatResponseModeDraft] = useState("executive");
  const [sessions, setSessions] = useState([]);
  const [sessionsLoading, setSessionsLoading] = useState(false);
  const [pendingSessionRevoke, setPendingSessionRevoke] = useState(null);

  const loadStatus = async () => {
    setLoading(true);
    try {
      const data = await getMfaStatus();
      setMfa(data.mfa || null);
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.mfa", "Falha ao carregar MFA"), error.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStatus();
    loadLlmAdmin();
    if (isGlobalSuperadmin) {
      loadSmtpConfig();
    } else {
      loadTenantLlmConfig();
      loadUserPreference();
      loadSessions();
    }
  }, [isGlobalSuperadmin]);

  const loadLlmAdmin = async () => {
    try {
      const [providersData, configData] = await Promise.all([listAdminLlmProviders(), getAdminLlmConfig()]);
      const providers = providersData.providers || [];
      setLlmProviders(providers);
      await loadAdminModelsCatalog(providers);
      setLlmConfig(configData.config || null);
      setLlmDenied(false);
    } catch (error) {
      setLlmDenied(true);
    }
  };

  const loadSmtpConfig = async () => {
    try {
      const data = await getAdminSmtpConfig();
      setSmtpConfig(data.config || null);
    } catch (error) {
      onSystemMessage("error", "Falha ao carregar SMTP", error.message);
    }
  };

  const loadTenantLlmConfig = async () => {
    try {
      const [providersData, cfgData] = await Promise.all([listTenantLlmProviders(), getTenantLlmConfig()]);
      const providers = providersData.providers || [];
      setTenantLlmProviders(providers);
      await loadTenantModelsCatalog(providers);
      setTenantLlmConfig(cfgData.config || null);
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.tenantLlm", "Falha ao carregar LLM do tenant"), error.message);
    }
  };

  const loadAdminModelsCatalog = async (providers) => {
    const safeProviders = Array.isArray(providers) ? providers : [];
    const rows = await Promise.all(
      safeProviders.map(async (item) => {
        try {
          const data = await listAdminLlmModels(item.code);
          return [item.code, Array.isArray(data.models) ? data.models : []];
        } catch (_) {
          return [item.code, []];
        }
      })
    );
    setLlmModelsByProvider(Object.fromEntries(rows));
  };

  const loadTenantModelsCatalog = async (providers) => {
    const safeProviders = Array.isArray(providers) ? providers : [];
    const rows = await Promise.all(
      safeProviders.map(async (item) => {
        try {
          const data = await listTenantLlmModels(item.code);
          return [item.code, Array.isArray(data.models) ? data.models : []];
        } catch (_) {
          return [item.code, []];
        }
      })
    );
    setTenantLlmModelsByProvider(Object.fromEntries(rows));
  };

  const loadUserPreference = async () => {
    try {
      const data = await getUserTenantPreference();
      const pref = data.preference || null;
      setUserPref(pref);
      setLanguageDraft(pref?.language_code || "pt-BR");
      setThemeDraft(pref?.theme_code || "light");
      setChatResponseModeDraft(pref?.chat_response_mode || "executive");
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.pref", "Falha ao carregar preferencias"), error.message);
    }
  };

  const startSetup = async () => {
    setSubmitting(true);
    try {
      const data = await beginMfaSetup({ issuer: "IAOps Governance" });
      setSetupInfo(data.setup);
      setModalMode("enable");
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.startMfa", "Falha ao iniciar setup MFA"), error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitEnable = async ({ otp_code }) => {
    setSubmitting(true);
    try {
      await enableMfa({ otp_code });
      setModalMode(null);
      setSetupInfo(null);
      onSystemMessage(
        "success",
        tUi("config.ok.mfaEnable.title", "MFA habilitado"),
        tUi("config.ok.mfaEnable.message", "MFA TOTP habilitado com sucesso.")
      );
      await loadStatus();
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.enableMfa", "Falha ao habilitar MFA"), error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitDisable = async ({ otp_code }) => {
    setSubmitting(true);
    try {
      await disableMfa({ otp_code });
      setModalMode(null);
      setSetupInfo(null);
      onSystemMessage(
        "success",
        tUi("config.ok.mfaDisable.title", "MFA desabilitado"),
        tUi("config.ok.mfaDisable.message", "MFA desabilitado para seu usuario.")
      );
      await loadStatus();
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.disableMfa", "Falha ao desabilitar MFA"), error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitLlmConfig = async (payload) => {
    setSubmitting(true);
    try {
      const data = await updateAdminLlmConfig(payload);
      setLlmConfig(data.config || null);
      setLlmModalOpen(false);
      onSystemMessage(
        "success",
        tUi("config.ok.appLlm.title", "LLM do app atualizada"),
        tUi("config.ok.appLlm.message", "Configuracao da LLM padrao atualizada com sucesso.")
      );
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.appLlm", "Falha ao atualizar LLM do app"), error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitSmtpConfig = async (payload) => {
    setSubmitting(true);
    try {
      const data = await updateAdminSmtpConfig(payload);
      setSmtpConfig(data.config || null);
      setSmtpModalOpen(false);
      onSystemMessage("success", "SMTP atualizado", "Configuracao SMTP salva com sucesso.");
    } catch (error) {
      onSystemMessage("error", "Falha ao atualizar SMTP", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const validateSmtpConfig = async (payload, options = {}) => {
    setSubmitting(true);
    try {
      if (options?.sendMail) {
        const data = await sendAdminSmtpTestEmail(payload);
        onSystemMessage("success", "E-mail de teste enviado", data?.message || "Envio de teste realizado com sucesso.");
      } else {
        await testAdminSmtpConfig(payload);
        onSystemMessage("success", "SMTP validado", "Conexao SMTP validada com sucesso.");
      }
    } catch (error) {
      onSystemMessage("error", options?.sendMail ? "Falha no envio de teste" : "Falha na validacao SMTP", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitTenantLlmConfig = async (payload) => {
    setSubmitting(true);
    try {
      const data = await updateTenantLlmConfig(payload);
      setTenantLlmConfig(data.config || null);
      setTenantLlmModalOpen(false);
      onSystemMessage(
        "success",
        tUi("config.ok.tenantLlm.title", "LLM do tenant atualizada"),
        tUi("config.ok.tenantLlm.message", "Configuracao de LLM do tenant salva com sucesso.")
      );
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.saveTenantLlm", "Falha ao salvar LLM do tenant"), error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const saveUserPreference = async () => {
    setSubmitting(true);
    try {
      const data = await updateUserTenantPreference({
        language_code: languageDraft,
        theme_code: themeDraft,
        chat_response_mode: chatResponseModeDraft,
      });
      const pref = data.preference || null;
      setUserPref(pref);
      onPreferenceApplied?.(pref);
      onSystemMessage(
        "success",
        tUi("config.ok.pref.title", "Preferencias salvas"),
        tUi("config.ok.pref.message", "Idioma, tema e modo de resposta foram atualizados.")
      );
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.savePref", "Falha ao salvar preferencia"), error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const loadSessions = async () => {
    setSessionsLoading(true);
    try {
      const data = await listAuthSessions();
      const current = getAuthContext();
      const scoped =
        data.scope === "client"
          ? (data.sessions || []).filter((item) => Number(item.user_id) === Number(current?.user_id))
          : data.sessions || [];
      setSessions(scoped);
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.sessions", "Falha ao carregar sessoes"), error.message);
    } finally {
      setSessionsLoading(false);
    }
  };

  const formatEpoch = (value) => {
    const num = Number(value || 0);
    if (!num) return "-";
    return new Date(num * 1000).toLocaleString();
  };

  const confirmRevokeSession = async () => {
    if (!pendingSessionRevoke) return;
    setSubmitting(true);
    try {
      await revokeAuthSession({ session_token: pendingSessionRevoke.session_token });
      const wasCurrent = Boolean(pendingSessionRevoke.is_current);
      setPendingSessionRevoke(null);
      onSystemMessage(
        "success",
        tUi("config.ok.revokeSession.title", "Sessao encerrada"),
        tUi("config.ok.revokeSession.message", "Sessao revogada com sucesso.")
      );
      if (wasCurrent) {
        onSessionRevokedCurrent?.();
        return;
      }
      await loadSessions();
    } catch (error) {
      onSystemMessage("error", tUi("config.fail.revokeSession", "Falha ao revogar sessao"), error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const revokeAllMySessions = async () => {
    const current = getAuthContext();
    const mySessions = (sessions || []).filter((item) => Number(item.user_id) === Number(current?.user_id));
    if (mySessions.length === 0) {
      onSystemMessage("warning", "Sessoes", "Nenhuma sessao ativa encontrada para seu usuario.");
      return;
    }
    setSubmitting(true);
    try {
      const nonCurrent = mySessions.filter((item) => !item.is_current);
      for (const item of nonCurrent) {
        await revokeAuthSession({ session_token: item.session_token });
      }
      const currentSession = mySessions.find((item) => item.is_current);
      if (currentSession) {
        await revokeAuthSession({ session_token: currentSession.session_token });
        onSessionRevokedCurrent?.();
        return;
      }
      await loadSessions();
      onSystemMessage("success", "Sessoes encerradas", "Todas as sessoes do seu usuario foram encerradas.");
    } catch (error) {
      onSystemMessage("error", "Falha ao encerrar sessoes", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>{tUi("config.header.title", "Configuracao")}</h2>
        <p>{tUi("config.header.subtitle", "MFA por usuario com TOTP (ativacao voluntaria).")}</p>
      </header>
      {loading ? (
        <p className="empty-state">{tUi("config.loading.mfa", "Carregando configuracao MFA...")}</p>
      ) : (
        <>
          <div className="metric-grid">
            <article className="metric-card">
              <h4>{tUi("config.mfa.status", "Status MFA")}</h4>
              <p>{mfa?.enabled ? tUi("access.mfa.enabled", "Habilitado") : tUi("access.mfa.disabled", "Desabilitado")}</p>
            </article>
            <article className="metric-card">
              <h4>{tUi("config.mfa.pending", "Setup pendente")}</h4>
              <p>{mfa?.has_pending_setup ? tUi("common.yes", "Sim") : tUi("common.no", "Nao")}</p>
            </article>
          </div>

          <div className="page-actions">
            <button type="button" className="btn btn-primary" onClick={startSetup} disabled={submitting}>
              {tUi("config.mfa.enable", "Ativar MFA")}
            </button>
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => setModalMode("disable")}
              disabled={submitting || !mfa?.enabled}
            >
              {tUi("config.mfa.disable", "Desativar MFA")}
            </button>
            <button type="button" className="btn btn-secondary" onClick={loadStatus}>
              {tUi("config.refresh.status", "Atualizar Status")}
            </button>
          </div>
        </>
      )}

      {!isGlobalSuperadmin && (
      <section className="catalog-block">
        <header>
          <h3>{tUi("config.pref.title", "Preferencias Usuario + Tenant")}</h3>
        </header>
        <div className="table-wrap">
          <table className="data-table">
            <tbody>
              <tr>
                <th>{tUi("config.pref.currentMode", "Modo atual (Chat BI)")}</th>
                <td>{userPref?.chat_response_mode || "-"}</td>
              </tr>
              <tr>
                <th>{tUi("config.pref.currentLanguage", "Idioma atual")}</th>
                <td>{userPref?.language_code || "-"}</td>
              </tr>
              <tr>
                <th>{tUi("config.pref.currentTheme", "Tema atual")}</th>
                <td>{userPref?.theme_code || "-"}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div className="inline-form">
          <select value={languageDraft} onChange={(event) => setLanguageDraft(event.target.value)}>
            <option value="pt-BR">Portugues (Brasil)</option>
            <option value="en-US">English (US)</option>
            <option value="es-ES">Espanol</option>
          </select>
          <select value={themeDraft} onChange={(event) => setThemeDraft(event.target.value)}>
            <option value="light">Light</option>
            <option value="ocean">Ocean</option>
          </select>
          <select value={chatResponseModeDraft} onChange={(event) => setChatResponseModeDraft(event.target.value)}>
            <option value="executive">{tUi("chat.mode.executive", "Executiva")}</option>
            <option value="detailed">{tUi("chat.mode.detailed", "Detalhada")}</option>
          </select>
          <button type="button" className="btn btn-primary" onClick={saveUserPreference} disabled={submitting}>
            {tUi("config.pref.save", "Salvar Preferencias")}
          </button>
          <button type="button" className="btn btn-secondary" onClick={loadUserPreference} disabled={submitting}>
            {tUi("config.pref.refresh", "Atualizar Preferencia")}
          </button>
        </div>
      </section>
      )}

      {!isGlobalSuperadmin && (
      <section className="catalog-block">
        <div className="section-header">
          <h3>{tUi("config.sessions.title", "Sessoes do usuario")}</h3>
          <div className="chip-row">
            <button type="button" className="btn btn-secondary btn-small" onClick={loadSessions} disabled={submitting}>
              {tUi("common.refresh", "Atualizar")}
            </button>
            <button type="button" className="btn btn-secondary btn-small" onClick={revokeAllMySessions} disabled={submitting}>
              Encerrar todas
            </button>
          </div>
        </div>
        {sessionsLoading ? (
          <p className="empty-state">{tUi("config.sessions.loading", "Carregando sessoes...")}</p>
        ) : sessions.length === 0 ? (
          <p className="empty-state">{tUi("config.sessions.empty", "Nenhuma sessao ativa para este usuario.")}</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>{tUi("config.sessions.col.tenant", "Tenant")}</th>
                  <th>{tUi("config.sessions.col.issued", "Emitida em")}</th>
                  <th>{tUi("config.sessions.col.lastSeen", "Ultima atividade")}</th>
                  <th>{tUi("config.sessions.col.expires", "Expira em")}</th>
                  <th>{tUi("config.sessions.col.actions", "Acoes")}</th>
                </tr>
              </thead>
              <tbody>
                {sessions.map((item) => (
                  <tr key={item.session_token}>
                    <td>
                      {item.tenant_name || item.tenant_id}
                      {item.is_current ? tUi("config.sessions.currentRowTag", " (atual)") : ""}
                    </td>
                    <td>{formatEpoch(item.issued_at_epoch)}</td>
                    <td>{formatEpoch(item.last_seen_epoch)}</td>
                    <td>{formatEpoch(item.session_expires_at_epoch)}</td>
                    <td>
                      <button
                        type="button"
                        className="btn btn-small btn-secondary"
                        onClick={() => setPendingSessionRevoke(item)}
                        disabled={submitting}
                      >
                        {item.is_current
                          ? tUi("config.sessions.endCurrent", "Encerrar atual")
                          : tUi("config.sessions.end", "Encerrar")}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
      )}

      {!isGlobalSuperadmin && (
      <section className="catalog-block">
        <header>
          <h3>{tUi("config.tenantLlm.title", "LLM do Tenant")}</h3>
        </header>
        <div className="table-wrap">
          <table className="data-table">
            <tbody>
              <tr>
                <th>{tUi("config.tenantLlm.useApp", "Usar LLM do app")}</th>
                <td>{tenantLlmConfig?.use_app_default_llm ? tUi("common.yes", "Sim") : tUi("common.no", "Nao")}</td>
              </tr>
              <tr>
                <th>Provedor</th>
                <td>{tenantLlmConfig?.provider_name || "-"}</td>
              </tr>
              <tr>
                <th>Modelo</th>
                <td>{tenantLlmConfig?.model_code || "-"}</td>
              </tr>
              <tr>
                <th>Endpoint</th>
                <td>{tenantLlmConfig?.endpoint_url || "-"}</td>
              </tr>
            </tbody>
          </table>
        </div>
        {!tenantLlmConfig?.use_app_default_llm && !tenantLlmConfig?.provider_name && !tenantLlmConfig?.model_code ? (
          <p className="empty-state">
            Nenhuma LLM selecionada para o tenant. Clique em "Configurar LLM do Tenant" e escolha "Usar LLM padrao do app" ou configure uma LLM propria.
          </p>
        ) : null}
        <div className="page-actions">
          <button type="button" className="btn btn-primary" onClick={() => setTenantLlmModalOpen(true)} disabled={submitting}>
            {tUi("config.tenantLlm.configure", "Configurar LLM do Tenant")}
          </button>
          <button type="button" className="btn btn-secondary" onClick={loadTenantLlmConfig}>
            {tUi("config.tenantLlm.refresh", "Atualizar LLM Tenant")}
          </button>
        </div>
      </section>
      )}

      {isGlobalSuperadmin && (
      <section className="catalog-block">
        <header>
          <h3>SMTP do App (Superadmin)</h3>
        </header>
        <div className="table-wrap">
          <table className="data-table">
            <tbody>
              <tr>
                <th>Host</th>
                <td>{smtpConfig?.host || "-"}</td>
              </tr>
              <tr>
                <th>Porta</th>
                <td>{smtpConfig?.port || "-"}</td>
              </tr>
              <tr>
                <th>Usuario</th>
                <td>{smtpConfig?.user || "-"}</td>
              </tr>
              <tr>
                <th>Remetente</th>
                <td>{smtpConfig?.from_email || "-"}</td>
              </tr>
              <tr>
                <th>STARTTLS</th>
                <td>{smtpConfig?.starttls ? "Sim" : "Nao"}</td>
              </tr>
              <tr>
                <th>Senha configurada</th>
                <td>{smtpConfig?.password_set ? "Sim" : "Nao"}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div className="page-actions">
          <button type="button" className="btn btn-primary" onClick={() => setSmtpModalOpen(true)} disabled={submitting}>
            Configurar SMTP
          </button>
          <button type="button" className="btn btn-secondary" onClick={loadSmtpConfig}>
            Atualizar SMTP
          </button>
        </div>
      </section>
      )}

      {isGlobalSuperadmin && (
      <section className="catalog-block">
        <header>
          <h3>{tUi("config.appLlm.title", "LLM Padrao do App (Superadmin)")}</h3>
        </header>
        {llmDenied ? (
          <p className="empty-state">{tUi("config.appLlm.denied", "Acesso restrito a superadmin.")}</p>
        ) : (
          <>
            <div className="table-wrap">
              <table className="data-table">
                <tbody>
                  <tr>
                    <th>Provedor</th>
                    <td>{llmConfig?.provider_name || "-"}</td>
                  </tr>
                  <tr>
                    <th>Modelo</th>
                    <td>{llmConfig?.model_code || "-"}</td>
                  </tr>
                  <tr>
                    <th>Endpoint</th>
                    <td>{llmConfig?.endpoint_url || "-"}</td>
                  </tr>
                  <tr>
                    <th>Secret Ref</th>
                    <td>{llmConfig?.secret_ref || "-"}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div className="page-actions">
              <button type="button" className="btn btn-primary" onClick={() => setLlmModalOpen(true)} disabled={submitting}>
                {tUi("config.appLlm.configure", "Configurar LLM do App")}
              </button>
              <button type="button" className="btn btn-secondary" onClick={loadLlmAdmin}>
                {tUi("config.appLlm.refresh", "Atualizar LLM")}
              </button>
            </div>
          </>
        )}
      </section>
      )}

      <MfaCodeModal
        open={modalMode === "enable"}
        title={tUi("config.modal.enable.title", "Ativar MFA TOTP")}
        subtitle={tUi("config.modal.enable.subtitle", "Escaneie o secret/URI no app autenticador e informe o codigo para ativar.")}
        setupInfo={setupInfo}
        submitLabel={tUi("config.modal.enable.confirm", "Confirmar Ativacao")}
        loading={submitting}
        onClose={() => {
          if (!submitting) setModalMode(null);
        }}
        onSubmit={submitEnable}
      />

      <MfaCodeModal
        open={modalMode === "disable"}
        title={tUi("config.modal.disable.title", "Desativar MFA TOTP")}
        subtitle={tUi("config.modal.disable.subtitle", "Informe o codigo TOTP atual para confirmar a desativacao.")}
        setupInfo={null}
        submitLabel={tUi("config.modal.disable.confirm", "Confirmar Desativacao")}
        loading={submitting}
        onClose={() => {
          if (!submitting) setModalMode(null);
        }}
        onSubmit={submitDisable}
      />

      <AppLlmConfigModal
        open={llmModalOpen}
        providers={llmProviders}
        modelsByProvider={llmModelsByProvider}
        initialConfig={llmConfig}
        loading={submitting}
        onRefreshCatalog={loadLlmAdmin}
        onClose={() => {
          if (!submitting) setLlmModalOpen(false);
        }}
        onSubmit={submitLlmConfig}
      />

      <SmtpConfigModal
        open={smtpModalOpen}
        initialConfig={smtpConfig}
        loading={submitting}
        onClose={() => {
          if (!submitting) setSmtpModalOpen(false);
        }}
        onSubmit={submitSmtpConfig}
        onTest={validateSmtpConfig}
      />

      <TenantLlmConfigModal
        open={tenantLlmModalOpen}
        providers={tenantLlmProviders}
        modelsByProvider={tenantLlmModelsByProvider}
        initialConfig={tenantLlmConfig}
        loading={submitting}
        onRefreshCatalog={loadTenantLlmConfig}
        onClose={() => {
          if (!submitting) setTenantLlmModalOpen(false);
        }}
        onSubmit={submitTenantLlmConfig}
      />

      <ConfirmActionModal
        open={Boolean(pendingSessionRevoke)}
        title={tUi("config.sessions.modal.title", "Encerrar sessao")}
        message={
          pendingSessionRevoke
            ? tUi("config.sessions.modal.message", "Deseja encerrar esta sessao{current}?", {
                current: pendingSessionRevoke.is_current ? tUi("config.sessions.currentTag", " atual") : "",
              })
            : ""
        }
        confirmLabel={tUi("common.confirm", "Confirmar")}
        loading={submitting}
        onConfirm={confirmRevokeSession}
        onClose={() => {
          if (!submitting) setPendingSessionRevoke(null);
        }}
      />
    </section>
  );
}

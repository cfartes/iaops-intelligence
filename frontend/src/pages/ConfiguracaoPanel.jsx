import { useEffect, useState } from "react";
import {
  beginMfaSetup,
  channelGetActiveTenant,
  channelListUserTenants,
  channelSelectTenant,
  channelWebhookTelegram,
  channelWebhookWhatsapp,
  disableMfa,
  listAccessUsers,
  listChannelBindings,
  upsertChannelBinding,
  deleteChannelBinding,
  enableMfa,
  getAdminLlmConfig,
  getAdminSmtpConfig,
  getMfaStatus,
  getUserTenantPreference,
  getTenantLlmConfig,
  getAuthContext,
  listClientTenants,
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

export default function ConfiguracaoPanel({ onSystemMessage, onNavigate, onPreferenceApplied, onSessionRevokedCurrent }) {
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
  const [channelType, setChannelType] = useState("telegram");
  const [externalUserKey, setExternalUserKey] = useState("");
  const [conversationKey, setConversationKey] = useState("");
  const [messageText, setMessageText] = useState("tenant list");
  const [webhookResponse, setWebhookResponse] = useState(null);
  const [tenantOptions, setTenantOptions] = useState([]);
  const [selectedTenantId, setSelectedTenantId] = useState("");
  const [activeTenantLabel, setActiveTenantLabel] = useState("");
  const [isSendingChannelMessage, setIsSendingChannelMessage] = useState(false);
  const [isLoadingChannelTenants, setIsLoadingChannelTenants] = useState(false);
  const [isLoadingChannelActiveTenant, setIsLoadingChannelActiveTenant] = useState(false);
  const [isSelectingChannelTenant, setIsSelectingChannelTenant] = useState(false);
  const [channelBindings, setChannelBindings] = useState([]);
  const [tenantItems, setTenantItems] = useState([]);
  const [channelUsers, setChannelUsers] = useState([]);
  const [isLoadingChannelBindings, setIsLoadingChannelBindings] = useState(false);
  const [selectedBindingId, setSelectedBindingId] = useState("");
  const [channelBindingDraft, setChannelBindingDraft] = useState({
    tenant_id: String(auth?.tenant_id || ""),
    user_id: "",
    channel_type: "telegram",
    external_user_key: "",
    is_active: true,
  });

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
      loadChannelTenantsCatalog();
      loadChannelBindings();
      loadChannelUsers();
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

  const loadChannelUsers = async () => {
    try {
      const data = await listAccessUsers();
      const users = data.users || [];
      setChannelUsers(users);
      setChannelBindingDraft((prev) => {
        if (prev.user_id && users.some((item) => String(item.user_id) === String(prev.user_id))) return prev;
        return { ...prev, user_id: users[0] ? String(users[0].user_id) : String(auth?.user_id || "") };
      });
    } catch (_) {
      setChannelUsers([]);
    }
  };

  const loadChannelTenantsCatalog = async () => {
    try {
      const data = await listClientTenants();
      const rows = data.tenants || [];
      setTenantItems(rows);
      setChannelBindingDraft((prev) => {
        if (prev.tenant_id && rows.some((item) => String(item.id) === String(prev.tenant_id))) return prev;
        return { ...prev, tenant_id: rows[0] ? String(rows[0].id) : "" };
      });
    } catch (_) {
      setTenantItems([]);
    }
  };

  const loadChannelBindings = async () => {
    setIsLoadingChannelBindings(true);
    try {
      const data = await listChannelBindings();
      const bindings = data.bindings || [];
      setChannelBindings(bindings);
      const selected = selectedBindingId
        ? bindings.find((item) => String(item.id) === String(selectedBindingId))
        : null;
      const activeDefault = selected || bindings.find((item) => Boolean(item.is_active)) || null;
      if (activeDefault) {
        setSelectedBindingId(String(activeDefault.id));
        setChannelType(String(activeDefault.channel_type || "telegram"));
        setExternalUserKey(String(activeDefault.external_user_key || ""));
        if (!conversationKey.trim()) setConversationKey(String(activeDefault.external_user_key || ""));
      } else {
        setSelectedBindingId("");
      }
    } catch (error) {
      onSystemMessage("error", "Falha ao listar vinculos de canal", error.message);
    } finally {
      setIsLoadingChannelBindings(false);
    }
  };

  const saveChannelBinding = async () => {
    const tenantId = Number(channelBindingDraft.tenant_id || 0);
    const userId = Number(channelBindingDraft.user_id || 0);
    const externalUserKey = String(channelBindingDraft.external_user_key || "").trim();
    const channelTypeDraft = String(channelBindingDraft.channel_type || "").trim().toLowerCase();
    if (!Number.isFinite(tenantId) || tenantId <= 0) {
      onSystemMessage("warning", "Campos obrigatorios", "Selecione o tenant.");
      return;
    }
    if (!externalUserKey) {
      onSystemMessage("warning", "Campos obrigatorios", "Informe o identificador do usuario no canal.");
      return;
    }
    setSubmitting(true);
    try {
      await upsertChannelBinding({
        tenant_id: tenantId,
        user_id: Number.isFinite(userId) && userId > 0 ? userId : null,
        channel_type: channelTypeDraft,
        external_user_key: externalUserKey,
        is_active: Boolean(channelBindingDraft.is_active),
      });
      onSystemMessage("success", "Vinculo salvo", "Identidade do canal vinculada ao tenant com sucesso.");
      setChannelBindingDraft((prev) => ({ ...prev, external_user_key: "" }));
      await loadChannelBindings();
      if (String(channelTypeDraft) === String(channelType)) {
        const updated = await listChannelBindings();
        const rows = updated.bindings || [];
        const matched = rows.find(
          (item) =>
            String(item.channel_type || "") === String(channelTypeDraft) &&
            String(item.external_user_key || "") === String(externalUserKey)
        );
        if (matched) setSelectedBindingId(String(matched.id));
        setExternalUserKey(externalUserKey);
      }
    } catch (error) {
      onSystemMessage("error", "Falha ao salvar vinculo", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const removeChannelBinding = async (bindingId) => {
    if (!bindingId) return;
    setSubmitting(true);
    try {
      await deleteChannelBinding({ binding_id: Number(bindingId) });
      onSystemMessage("success", "Vinculo removido", "Vinculo de canal removido.");
      await loadChannelBindings();
    } catch (error) {
      onSystemMessage("error", "Falha ao remover vinculo", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const toggleChannelBinding = async (item) => {
    if (!item) return;
    setSubmitting(true);
    try {
      await upsertChannelBinding({
        tenant_id: Number(item.tenant_id),
        user_id: item.user_id != null ? Number(item.user_id) : null,
        channel_type: String(item.channel_type || "").toLowerCase(),
        external_user_key: String(item.external_user_key || ""),
        is_active: !Boolean(item.is_active),
      });
      onSystemMessage("success", "Vinculo atualizado", "Status do vinculo atualizado com sucesso.");
      await loadChannelBindings();
    } catch (error) {
      onSystemMessage("error", "Falha ao atualizar vinculo", error.message);
    } finally {
      setSubmitting(false);
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

  const baseChannelPayload = () => ({
    channel_type: channelType,
    external_user_key: externalUserKey.trim(),
    conversation_key: conversationKey.trim(),
  });

  const ensureChannelKeys = () => {
    if (!externalUserKey.trim() || !conversationKey.trim()) {
      onSystemMessage(
        "warning",
        tUi("op.required.title", "Campos obrigatorios"),
        tUi("op.required.message", "Informe o identificador do usuario no canal e o identificador da conversa.")
      );
      return false;
    }
    return true;
  };

  const loadChannelTenants = async () => {
    if (!ensureChannelKeys()) return;
    setIsLoadingChannelTenants(true);
    try {
      const data = await channelListUserTenants(baseChannelPayload());
      const tenants = data.tenants || [];
      setTenantOptions(tenants);
      setSelectedTenantId((prev) => {
        if (prev && tenants.some((item) => String(item.tenant_id) === String(prev))) return prev;
        return tenants[0] ? String(tenants[0].tenant_id) : "";
      });
      onSystemMessage(
        "success",
        tUi("op.tenant.loaded.title", "Tenants carregados"),
        tUi("op.tenant.loaded.message", "{count} tenant(s) disponivel(is) para este usuario/canal.", {
          count: tenants.length,
        })
      );
    } catch (error) {
      onSystemMessage("error", tUi("op.tenant.fail.title", "Falha na gestao de tenant"), error.message);
    } finally {
      setIsLoadingChannelTenants(false);
    }
  };

  const loadChannelActiveTenant = async () => {
    if (!ensureChannelKeys()) return;
    setIsLoadingChannelActiveTenant(true);
    try {
      const data = await channelGetActiveTenant(baseChannelPayload());
      const activeTenantId = data.active_tenant_id;
      const tenants = data.tenants || tenantOptions;
      if (tenants.length > 0 && tenantOptions.length === 0) setTenantOptions(tenants);
      if (activeTenantId == null) {
        setActiveTenantLabel(tUi("op.tenant.active.none", "Nenhum tenant ativo na conversa."));
        return;
      }
      const selected = tenants.find((item) => String(item.tenant_id) === String(activeTenantId));
      const label = selected
        ? `${selected.tenant_id} - ${selected.name} (${selected.status}, ${selected.role})`
        : tUi("op.tenant.active.onlyId", "Tenant ativo: {tenant_id}", { tenant_id: activeTenantId });
      setActiveTenantLabel(label);
      setSelectedTenantId(String(activeTenantId));
    } catch (error) {
      onSystemMessage("error", tUi("op.tenant.fail.title", "Falha na gestao de tenant"), error.message);
    } finally {
      setIsLoadingChannelActiveTenant(false);
    }
  };

  const selectChannelActiveTenant = async () => {
    if (!ensureChannelKeys()) return;
    if (!selectedTenantId) {
      onSystemMessage(
        "warning",
        tUi("op.required.title", "Campos obrigatorios"),
        tUi("op.tenant.select.required", "Selecione um tenant para ativar no canal.")
      );
      return;
    }
    setIsSelectingChannelTenant(true);
    try {
      await channelSelectTenant({
        ...baseChannelPayload(),
        tenant_id: Number(selectedTenantId),
      });
      await loadChannelActiveTenant();
      onSystemMessage(
        "success",
        tUi("op.tenant.select.ok.title", "Tenant ativo atualizado"),
        tUi("op.tenant.select.ok.message", "Tenant da conversa atualizado com sucesso.")
      );
    } catch (error) {
      onSystemMessage("error", tUi("op.tenant.fail.title", "Falha na gestao de tenant"), error.message);
    } finally {
      setIsSelectingChannelTenant(false);
    }
  };

  const sendChannelMessage = async () => {
    if (!ensureChannelKeys()) return;
    setWebhookResponse(null);
    setIsSendingChannelMessage(true);
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
      onSystemMessage(
        "success",
        tUi("op.webhook.ok.title", "Webhook processado"),
        tUi("op.webhook.ok.message", "Mensagem processada com sucesso no canal.")
      );
    } catch (error) {
      onSystemMessage("error", tUi("op.webhook.fail.title", "Erro no webhook"), error.message);
    } finally {
      setIsSendingChannelMessage(false);
    }
  };

  useEffect(() => {
    const selected = selectedBindingId
      ? (channelBindings || []).find((item) => String(item.id) === String(selectedBindingId))
      : null;
    if (selected) {
      setChannelType(String(selected.channel_type || "telegram"));
      const key = String(selected.external_user_key || "");
      setExternalUserKey(key);
      if (!conversationKey.trim()) setConversationKey(key);
    }
    setTenantOptions([]);
    setSelectedTenantId("");
    setActiveTenantLabel("");
    setWebhookResponse(null);
  }, [channelType, channelBindings, selectedBindingId]);

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
        <header>
          <h3>Vinculo Canal x Tenant</h3>
        </header>
        <p className="muted">
          Defina para qual tenant cada identidade de canal (Telegram/WhatsApp) deve apontar.
        </p>
        <p className="muted">Identificador do usuario no canal: Telegram (`from.id`/`chat_id`) ou WhatsApp (`from`). Usuario e opcional (auditoria).</p>
        <div className="inline-form">
          <select
            value={channelBindingDraft.tenant_id}
            onChange={(event) =>
              setChannelBindingDraft((prev) => ({ ...prev, tenant_id: event.target.value }))
            }
          >
            <option value="">Selecione um tenant</option>
            {tenantItems.map((item) => (
              <option key={item.id} value={String(item.id)}>
                {`${item.name} (#${item.id})`}
              </option>
            ))}
          </select>
          <select
            value={channelBindingDraft.channel_type}
            onChange={(event) =>
              setChannelBindingDraft((prev) => ({ ...prev, channel_type: event.target.value }))
            }
          >
            <option value="telegram">Telegram</option>
            <option value="whatsapp">WhatsApp</option>
          </select>
          <select
            value={channelBindingDraft.user_id}
            onChange={(event) =>
              setChannelBindingDraft((prev) => ({ ...prev, user_id: event.target.value }))
            }
          >
            <option value="">Usuario (opcional)</option>
            {channelUsers.map((item) => (
              <option key={item.user_id} value={String(item.user_id)}>
                {`${item.full_name || item.email} (#${item.user_id})`}
              </option>
            ))}
          </select>
          <input
            value={channelBindingDraft.external_user_key}
            onChange={(event) =>
              setChannelBindingDraft((prev) => ({ ...prev, external_user_key: event.target.value }))
            }
            placeholder="Identificador do usuario no canal"
          />
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={Boolean(channelBindingDraft.is_active)}
              onChange={(event) =>
                setChannelBindingDraft((prev) => ({ ...prev, is_active: event.target.checked }))
              }
            />
            Ativo
          </label>
          <button type="button" className="btn btn-primary" onClick={saveChannelBinding} disabled={submitting}>
            Salvar vinculo
          </button>
          <button type="button" className="btn btn-secondary" onClick={loadChannelBindings} disabled={isLoadingChannelBindings}>
            {isLoadingChannelBindings ? "Atualizando..." : "Atualizar lista"}
          </button>
        </div>
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Canal</th>
                <th>Identificador no canal</th>
                <th>Tenant</th>
                <th>Usuario (opcional)</th>
                <th>Status</th>
                <th>Acoes</th>
              </tr>
            </thead>
            <tbody>
              {channelBindings.length === 0 ? (
                <tr>
                  <td colSpan={7} className="empty-state">Nenhum vinculo cadastrado.</td>
                </tr>
              ) : (
                channelBindings.map((item) => (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.channel_type}</td>
                    <td>{item.external_user_key}</td>
                    <td>{item.tenant_name || (item.tenant_id ? `#${item.tenant_id}` : "-")}</td>
                    <td>{item.user_full_name || item.user_email || (item.user_id ? `#${item.user_id}` : "-")}</td>
                    <td>{item.is_active ? "Ativo" : "Inativo"}</td>
                    <td>
                      <div className="table-actions">
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() => toggleChannelBinding(item)}
                          disabled={submitting}
                        >
                          {item.is_active ? "Inativar" : "Ativar"}
                        </button>
                        <button
                          type="button"
                          className="btn btn-small btn-secondary"
                          onClick={() => removeChannelBinding(item.id)}
                          disabled={submitting}
                        >
                          Remover
                        </button>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
      )}

      {!isGlobalSuperadmin && (
      <section className="catalog-block">
        <header>
          <h3>Canais do Tenant (Telegram/WhatsApp)</h3>
        </header>
        <p className="muted">
          Configure aqui o contexto do canal para este tenant e valide o fluxo de mensagens.
        </p>
        <p className="muted">Dica: selecione uma identidade de canal ja vinculada ao tenant.</p>
        <div className="inline-form">
          <select value={channelType} onChange={(event) => setChannelType(event.target.value)}>
            <option value="telegram">Telegram</option>
            <option value="whatsapp">WhatsApp</option>
          </select>
          <select
            value={selectedBindingId}
            onChange={(event) => {
              const bindingId = event.target.value;
              setSelectedBindingId(bindingId);
              const selected = (channelBindings || []).find((item) => String(item.id) === String(bindingId));
              if (!selected) return;
              const selectedChannel = String(selected.channel_type || "telegram");
              const key = String(selected.external_user_key || "");
              setChannelType(selectedChannel);
              setExternalUserKey(key);
              setConversationKey((prev) => (prev.trim() ? prev : key));
            }}
          >
            <option value="">Selecione identidade vinculada</option>
            {channelBindings
              .filter((item) => Boolean(item.is_active))
              .map((item) => (
                <option key={`${item.id}-${item.external_user_key}`} value={String(item.id)}>
                  {`${item.channel_type} | ${item.external_user_key} -> ${item.tenant_name || `#${item.tenant_id}`}`}
                </option>
              ))}
          </select>
          <input
            value={externalUserKey}
            onChange={(event) => setExternalUserKey(event.target.value)}
            placeholder="Identificador do usuario no canal"
          />
          <input
            value={conversationKey}
            onChange={(event) => setConversationKey(event.target.value)}
            placeholder="Identificador da conversa"
          />
        </div>
        <div className="inline-form">
          <button type="button" className="btn btn-secondary" onClick={loadChannelTenants} disabled={isLoadingChannelTenants}>
            {isLoadingChannelTenants ? tUi("op.tenant.loading", "Carregando...") : tUi("op.tenant.list", "Listar Tenants")}
          </button>
          <button type="button" className="btn btn-secondary" onClick={loadChannelActiveTenant} disabled={isLoadingChannelActiveTenant}>
            {isLoadingChannelActiveTenant ? tUi("op.tenant.loading", "Carregando...") : tUi("op.tenant.active.get", "Ver Tenant Ativo")}
          </button>
          <select value={selectedTenantId} onChange={(event) => setSelectedTenantId(event.target.value)}>
            <option value="">{tUi("op.tenant.select.placeholder", "Selecione um tenant")}</option>
            {tenantOptions.map((item) => (
              <option key={item.tenant_id} value={String(item.tenant_id)}>
                {`${item.tenant_id} - ${item.name} (${item.status}, ${item.role})`}
              </option>
            ))}
          </select>
          <button
            type="button"
            className="btn btn-primary"
            onClick={selectChannelActiveTenant}
            disabled={isSelectingChannelTenant || !selectedTenantId}
          >
            {isSelectingChannelTenant ? tUi("op.tenant.select.saving", "Atualizando...") : tUi("op.tenant.select.set", "Definir Tenant Ativo")}
          </button>
        </div>
        <article className="metric-card">
          <h4>{tUi("op.tenant.active.title", "Tenant ativo da conversa")}</h4>
          <p>{activeTenantLabel || tUi("op.tenant.active.none", "Nenhum tenant ativo na conversa.")}</p>
        </article>
        <div className="inline-form" style={{ marginTop: "0.75rem" }}>
          <input
            value={messageText}
            onChange={(event) => setMessageText(event.target.value)}
            placeholder={tUi("op.tester.message.placeholder", "Mensagem / comando")}
          />
          <button type="button" className="btn btn-primary" onClick={sendChannelMessage} disabled={isSendingChannelMessage}>
            {isSendingChannelMessage ? tUi("op.tester.sending", "Enviando...") : tUi("op.tester.send", "Enviar para Webhook")}
          </button>
        </div>
        {webhookResponse ? (
          <article className="metric-card" style={{ marginTop: "0.75rem" }}>
            <h4>{tUi("op.tester.reply", "Resposta do Bot")}</h4>
            <pre>{webhookResponse.reply_text || tUi("op.tester.noReply", "Sem resposta textual")}</pre>
          </article>
        ) : null}
        <div className="page-actions">
          <button
            type="button"
            className="btn btn-secondary"
            onClick={() => onNavigate?.("operacao")}
          >
            Abrir Operacao avancada
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

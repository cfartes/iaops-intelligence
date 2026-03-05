import { useEffect, useMemo, useRef, useState } from "react";
import { UI_TEXT } from "./translations";
import SideMenu from "./components/SideMenu";
import EntityFormModal from "./components/EntityFormModal";
import SystemMessageModal from "./components/SystemMessageModal";
import IncidentFormModal from "./components/IncidentFormModal";
import IncidentStatusModal from "./components/IncidentStatusModal";
import SetupAssistantModal, { ASSISTANT_STEPS } from "./components/SetupAssistantModal";
import AuthScreen from "./components/AuthScreen";
import PagePanel from "./pages/PagePanel";
import OnboardingPanel from "./pages/OnboardingPanel";
import InventoryPanel from "./pages/InventoryPanel";
import SuggestionsPanel from "./pages/SuggestionsPanel";
import IncidentPanel from "./pages/IncidentPanel";
import EventsPanel from "./pages/EventsPanel";
import OperationPanel from "./pages/OperationPanel";
import AuditPanel from "./pages/AuditPanel";
import SqlSecurityPanel from "./pages/SqlSecurityPanel";
import ChatBiPanel from "./pages/ChatBiPanel";
import AccessPanel from "./pages/AccessPanel";
import ConfiguracaoPanel from "./pages/ConfiguracaoPanel";
import LgpdPanel from "./pages/LgpdPanel";
import BillingPanel from "./pages/BillingPanel";
import { NAV_ITEMS } from "./state/nav";
import {
  createIncident,
  getMfaStatus,
  getOperationHealth,
  getTenantLlmConfig,
  getUserTenantPreference,
  listAccessUsers,
  listOnboardingMonitoredTables,
  listTenantDataSources,
  getSetupProgress,
  signupClient,
  confirmClientSignup,
  requestPasswordReset,
  confirmPasswordReset,
  loginClient,
  verifyLoginMfa,
  refreshAuthSession,
  logoutSession,
  getAuthContext as getStoredAuthContext,
  setAuthContext as setStoredAuthContext,
  clearAuthContext as clearStoredAuthContext,
  upsertSetupProgress,
  updateIncidentStatus,
  updateUserTenantPreference,
} from "./api/mcpApi";

const SETUP_PROGRESS_STORAGE_KEY = "iaops_setup_assistant_progress_v1";
const SETUP_DEFER_UNTIL_STORAGE_KEY = "iaops_setup_assistant_defer_until_v1";
const SETUP_MINI_CHECKLIST_COLLAPSED_KEY = "iaops_setup_mini_checklist_collapsed_v1";
const SETUP_DEFER_HOURS = 24;
const SESSION_INACTIVITY_TIMEOUT_MS = 5 * 60 * 1000;
const SESSION_REFRESH_AHEAD_MS = 2 * 60 * 1000;

export default function App() {
  const setupSyncSignatureRef = useRef("");
  const inactivityTimerRef = useRef(null);
  const sessionRefreshTimerRef = useRef(null);
  const [authContext, setAuthContext] = useState(() => getStoredAuthContext());
  const [activePage, setActivePage] = useState("onboarding");
  const [userTheme, setUserTheme] = useState("light");
  const [uiLanguage, setUiLanguage] = useState("pt-BR");
  const [isSetupAssistantOpen, setIsSetupAssistantOpen] = useState(false);
  const [completedSetupSteps, setCompletedSetupSteps] = useState([]);
  const [validatedSetupSteps, setValidatedSetupSteps] = useState([]);
  const [setupPendingReasons, setSetupPendingReasons] = useState({});
  const [miniChecklistCollapsed, setMiniChecklistCollapsed] = useState(false);
  const [isEntityModalOpen, setIsEntityModalOpen] = useState(false);
  const [isIncidentModalOpen, setIsIncidentModalOpen] = useState(false);
  const [isIncidentStatusModalOpen, setIsIncidentStatusModalOpen] = useState(false);
  const [selectedIncident, setSelectedIncident] = useState(null);
  const [incidentReloadSignal, setIncidentReloadSignal] = useState(0);
  const [messageModal, setMessageModal] = useState({
    open: false,
    tone: "info",
    title: "",
    message: "",
  });

  const languageBucket = useMemo(() => {
    const code = (uiLanguage || "pt-BR").toLowerCase();
    if (code.startsWith("en")) return "en";
    if (code.startsWith("es")) return "es";
    return "pt";
  }, [uiLanguage]);

  const uiText = useMemo(() => UI_TEXT[languageBucket], [languageBucket]);
  const isSuperadmin = useMemo(() => Boolean(authContext?.is_superadmin), [authContext?.is_superadmin]);
  const isGlobalSuperadmin = useMemo(
    () => isSuperadmin && Number(authContext?.tenant_id || 0) <= 0,
    [authContext?.is_superadmin, authContext?.tenant_id]
  );
  const localizedNavItems = useMemo(
    () =>
      NAV_ITEMS.filter((item) => {
        if (item.key === "seguranca-sql" && !isSuperadmin) return false;
        if (!isGlobalSuperadmin) return true;
        return ["configuracao", "seguranca-sql", "faturamento"].includes(item.key);
      }).map((item) => ({
        ...item,
        label: uiText.nav[item.key] || item.label,
      })),
    [isGlobalSuperadmin, isSuperadmin, uiText]
  );
  const localizedSubtitleByPage = useMemo(() => uiText.subtitles || {}, [uiText]);

  const activeLabel = useMemo(
    () => localizedNavItems.find((item) => item.key === activePage)?.label || uiText.genericModule,
    [activePage, localizedNavItems, uiText]
  );
  const currentSetupStep = useMemo(
    () => ASSISTANT_STEPS.find((step) => step.targetPage === activePage) || null,
    [activePage]
  );
  const effectiveCompletedSetupSteps = useMemo(
    () => Array.from(new Set([...completedSetupSteps, ...validatedSetupSteps])),
    [completedSetupSteps, validatedSetupSteps]
  );
  const pendingSetupStep = useMemo(
    () => ASSISTANT_STEPS.find((step) => !effectiveCompletedSetupSteps.includes(step.key)) || null,
    [effectiveCompletedSetupSteps]
  );
  const setupStepStatusByKey = useMemo(() => {
    const status = {};
    for (const step of ASSISTANT_STEPS) {
      if (effectiveCompletedSetupSteps.includes(step.key)) {
        status[step.key] = "done";
        continue;
      }
      const reason = setupPendingReasons[step.key] || "";
      const isPartial =
        step.key === "onboarding" &&
        reason === uiText.setupWizard.reasons.onboarding_no_table;
      const keepPendingWithReason = step.key === "operacao" && Boolean(reason);
      status[step.key] = isPartial ? "partial" : keepPendingWithReason ? "pending" : reason ? "blocked" : "pending";
    }
    return status;
  }, [effectiveCompletedSetupSteps, setupPendingReasons, uiText]);
  const setupStatusCounts = useMemo(() => {
    const counters = { done: 0, partial: 0, blocked: 0, pending: 0 };
    for (const step of ASSISTANT_STEPS) {
      const status = setupStepStatusByKey[step.key] || "pending";
      counters[status] = (counters[status] || 0) + 1;
    }
    return counters;
  }, [setupStepStatusByKey]);

  const openSystemMessage = (tone, title, message) => {
    setMessageModal({ open: true, tone, title, message });
  };

  const performLogout = ({ dueToInactivity = false, dueToInvalidSession = false, invalidSessionMessage = "" } = {}) => {
    const current = getStoredAuthContext();
    if (current?.refresh_token || current?.session_token) {
      logoutSession({
        refresh_token: current.refresh_token || undefined,
        session_token: current.session_token || undefined,
      }).catch(() => {
        // sem bloqueio de UX para logout local
      });
    }
    clearStoredAuthContext();
    setAuthContext(null);
    setIsSetupAssistantOpen(false);
    if (dueToInactivity) {
      openSystemMessage("warning", uiText.inactivityLogoutTitle, uiText.inactivityLogoutMessage);
    } else if (dueToInvalidSession) {
      openSystemMessage("warning", uiText.loginModal.fail_title, invalidSessionMessage || "Sessao invalida ou expirada.");
    }
  };

  useEffect(() => {
    const onInvalidSession = (event) => {
      const message = event?.detail?.message || "Sessao invalida ou expirada.";
      performLogout({ dueToInvalidSession: true, invalidSessionMessage: message });
    };
    window.addEventListener("iaops:invalid-session", onInvalidSession);
    return () => window.removeEventListener("iaops:invalid-session", onInvalidSession);
  }, [uiText.loginModal.fail_title]);

  useEffect(() => {
    if (!authContext) {
      if (inactivityTimerRef.current) {
        window.clearTimeout(inactivityTimerRef.current);
        inactivityTimerRef.current = null;
      }
      return undefined;
    }

    const resetInactivityTimer = () => {
      if (inactivityTimerRef.current) {
        window.clearTimeout(inactivityTimerRef.current);
      }
      inactivityTimerRef.current = window.setTimeout(() => {
        performLogout({ dueToInactivity: true });
      }, SESSION_INACTIVITY_TIMEOUT_MS);
    };

    const activityEvents = ["mousemove", "mousedown", "keydown", "scroll", "touchstart", "focus"];
    const handleVisibility = () => {
      if (document.visibilityState === "visible") {
        resetInactivityTimer();
      }
    };
    activityEvents.forEach((eventName) => window.addEventListener(eventName, resetInactivityTimer, { passive: true }));
    document.addEventListener("visibilitychange", handleVisibility);
    resetInactivityTimer();

    return () => {
      activityEvents.forEach((eventName) => window.removeEventListener(eventName, resetInactivityTimer));
      document.removeEventListener("visibilitychange", handleVisibility);
      if (inactivityTimerRef.current) {
        window.clearTimeout(inactivityTimerRef.current);
        inactivityTimerRef.current = null;
      }
    };
  }, [authContext, uiText.inactivityLogoutMessage, uiText.inactivityLogoutTitle]);

  useEffect(() => {
    if (sessionRefreshTimerRef.current) {
      window.clearTimeout(sessionRefreshTimerRef.current);
      sessionRefreshTimerRef.current = null;
    }
    if (!authContext?.refresh_token || !authContext?.session_expires_at_epoch) return undefined;

    const expiresAtMs = Number(authContext.session_expires_at_epoch) * 1000;
    const nowMs = Date.now();
    const delayMs = Math.max(15_000, expiresAtMs - nowMs - SESSION_REFRESH_AHEAD_MS);
    sessionRefreshTimerRef.current = window.setTimeout(async () => {
      try {
        const data = await refreshAuthSession({ refresh_token: authContext.refresh_token });
        const next = {
          ...(data?.auth_context || {}),
          ...(data?.profile || {}),
          ...(data?.session || {}),
        };
        setStoredAuthContext(next);
        setAuthContext(next);
      } catch (_) {
        performLogout();
      }
    }, delayMs);

    return () => {
      if (sessionRefreshTimerRef.current) {
        window.clearTimeout(sessionRefreshTimerRef.current);
        sessionRefreshTimerRef.current = null;
      }
    };
  }, [authContext]);

  useEffect(() => {
    if (!authContext) return;
    if (isGlobalSuperadmin) {
      setCompletedSetupSteps([]);
      setValidatedSetupSteps([]);
      setSetupPendingReasons({});
      setIsSetupAssistantOpen(false);
      return;
    }
    const rawCompleted = window.localStorage.getItem(SETUP_PROGRESS_STORAGE_KEY);
    const rawDeferred = window.localStorage.getItem(SETUP_DEFER_UNTIL_STORAGE_KEY);
    const rawCollapsed = window.localStorage.getItem(SETUP_MINI_CHECKLIST_COLLAPSED_KEY);
    let completed = [];
    if (rawCompleted) {
      try {
        const parsed = JSON.parse(rawCompleted);
        if (Array.isArray(parsed)) {
          completed = parsed;
        }
      } catch (_) {
        completed = [];
      }
    }
    const deferredUntil = rawDeferred ? Number(rawDeferred) : null;
    const collapsed = rawCollapsed === "1";
    setCompletedSetupSteps(completed);
    setMiniChecklistCollapsed(collapsed);

    const hasPending = ASSISTANT_STEPS.some((step) => !completed.includes(step.key));
    const canAutoOpen = !deferredUntil || Date.now() >= deferredUntil;
    if (hasPending && canAutoOpen) {
      setIsSetupAssistantOpen(true);
    }
    loadSetupProgressFromBackend();
    evaluateSetupProgress();
    loadUserPreference();
  }, [authContext, isGlobalSuperadmin]);

  useEffect(() => {
    if (!authContext) return;
    if (isGlobalSuperadmin) return;
    if (isSetupAssistantOpen) {
      evaluateSetupProgress();
    }
  }, [authContext, isSetupAssistantOpen, isGlobalSuperadmin]);

  useEffect(() => {
    if (!authContext) return;
    if (isGlobalSuperadmin) return;
    evaluateSetupProgress();
  }, [authContext, languageBucket, isGlobalSuperadmin]);

  useEffect(() => {
    document.documentElement.setAttribute("lang", uiLanguage || "pt-BR");
  }, [uiLanguage]);

  useEffect(() => {
    document.documentElement.dataset.theme = userTheme || "light";
  }, [userTheme]);

  const loadUserPreference = async () => {
    if (!authContext) return;
    try {
      const data = await getUserTenantPreference();
      const preference = data.preference || {};
      setUiLanguage(preference.language_code || "pt-BR");
      setUserTheme(preference.theme_code || "light");
    } catch (error) {
      openSystemMessage("warning", "Preferencias padrao", "Nao foi possivel carregar preferencias do usuario+tenant.");
    }
  };

  const loadSetupProgressFromBackend = async () => {
    if (!authContext) return;
    try {
      const data = await getSetupProgress();
      const progress = data?.progress || null;
      const snapshot = progress?.snapshot || {};
      const remoteCompleted = Array.isArray(snapshot.completed_steps) ? snapshot.completed_steps : [];
      if (remoteCompleted.length > 0) {
        markSetupStepsCompleted(remoteCompleted);
      }
    } catch (_) {
      // sem bloqueio de UX em caso de falha de sync backend
    }
  };

  const markSetupStepsCompleted = (stepKeys) => {
    if (!Array.isArray(stepKeys) || stepKeys.length === 0) return;
    setCompletedSetupSteps((prev) => {
      const merged = Array.from(new Set([...prev, ...stepKeys]));
      window.localStorage.setItem(SETUP_PROGRESS_STORAGE_KEY, JSON.stringify(merged));
      return merged;
    });
  };

  const evaluateSetupProgress = async () => {
    if (!authContext) return;
    try {
      const [sourcesData, usersData, tenantLlmData, mfaData, healthData] = await Promise.all([
        listTenantDataSources(),
        listAccessUsers(),
        getTenantLlmConfig(),
        getMfaStatus(),
        getOperationHealth(60),
      ]);

      const steps = [];
      const reasons = {};
      const reasonText = uiText.setupWizard.reasons;

      const sources = sourcesData?.sources || [];
      if (sources.length === 0) {
        reasons.onboarding = reasonText.onboarding_no_source;
      } else {
        const tablesData = await listOnboardingMonitoredTables();
        const tables = tablesData?.tables || [];
        if (tables.length === 0) {
          reasons.onboarding = reasonText.onboarding_no_table;
        } else {
          steps.push("onboarding");
        }
      }

      const users = usersData?.users || [];
      if (users.length > 0) {
        steps.push("acesso");
      } else {
        reasons.acesso = reasonText.acesso_no_users;
      }

      const tenantLlm = tenantLlmData?.config || {};
      const mfa = mfaData?.mfa || {};
      const hasLlmConfig = Boolean(tenantLlm.use_app_default_llm || tenantLlm.provider_name || tenantLlm.model_code);
      if (hasLlmConfig || mfa.enabled) {
        steps.push("configuracao");
      } else {
        reasons.configuracao = reasonText.configuracao_missing;
      }

      const channelsHealth = healthData?.channels_health || {};
      const hasChannelConfigured = Object.values(channelsHealth).some((status) => {
        const normalized = String(status || "").toLowerCase();
        return normalized && normalized !== "not_configured" && normalized !== "unknown";
      });
      if (hasChannelConfigured) {
        steps.push("operacao");
      } else {
        reasons.operacao = reasonText.operacao_missing_channels;
      }

      const unique = Array.from(new Set(steps));
      setValidatedSetupSteps(unique);
      setSetupPendingReasons(reasons);
      markSetupStepsCompleted(unique);
    } catch (_) {
      // Mantem fluxo do app mesmo com falha de validacao de progresso.
    }
  };

  const postponeAssistant = () => {
    const deferUntil = Date.now() + SETUP_DEFER_HOURS * 60 * 60 * 1000;
    window.localStorage.setItem(SETUP_DEFER_UNTIL_STORAGE_KEY, String(deferUntil));
    setIsSetupAssistantOpen(false);
  };

  const completeSetupStep = (stepKey) => {
    setCompletedSetupSteps((prev) => {
      if (prev.includes(stepKey)) return prev;
      const next = [...prev, stepKey];
      window.localStorage.setItem(SETUP_PROGRESS_STORAGE_KEY, JSON.stringify(next));
      if (next.length >= ASSISTANT_STEPS.length) {
        window.localStorage.removeItem(SETUP_DEFER_UNTIL_STORAGE_KEY);
        openSystemMessage("success", uiText.setupWizard.allDoneTitle, uiText.setupWizard.allDoneMessage);
        setIsSetupAssistantOpen(false);
      } else {
        openSystemMessage("success", uiText.setupWizard.stepDoneTitle, uiText.setupWizard.stepDoneMessage);
      }
      return next;
    });
  };

  useEffect(() => {
    if (!authContext) return;
    if (isGlobalSuperadmin) return;
    const snapshot = {
      completed_steps: effectiveCompletedSetupSteps,
      validated_steps: validatedSetupSteps,
      step_status: setupStepStatusByKey,
      pending_reasons: setupPendingReasons,
      counts: {
        done: setupStatusCounts.done,
        partial: setupStatusCounts.partial,
        blocked: setupStatusCounts.blocked,
        pending: setupStatusCounts.pending,
      },
      language_code: uiLanguage,
      active_page: activePage,
    };
    const signature = JSON.stringify(snapshot);
    if (signature === setupSyncSignatureRef.current) return;
    setupSyncSignatureRef.current = signature;
    upsertSetupProgress({ snapshot }).catch(() => {
      // sem bloqueio de UX em caso de falha de sync backend
    });
  }, [
    authContext,
    activePage,
    effectiveCompletedSetupSteps,
    isGlobalSuperadmin,
    setupPendingReasons,
    setupStatusCounts,
    setupStepStatusByKey,
    uiLanguage,
    validatedSetupSteps,
  ]);

  useEffect(() => {
    if (!authContext) return;
    if (!localizedNavItems.some((item) => item.key === activePage)) {
      setActivePage(localizedNavItems[0]?.key || "configuracao");
    }
  }, [activePage, authContext, localizedNavItems]);

  const handleEntityFormSubmit = (payload) => {
    const selectedLanguage = payload.languageCode || "pt-BR";
    setUiLanguage(selectedLanguage);
    setIsEntityModalOpen(false);
    updateUserTenantPreference({ language_code: selectedLanguage }).catch(() => {
      openSystemMessage("warning", "Preferencias", "Nao foi possivel persistir o idioma inicial.");
    });
    openSystemMessage(
      "success",
      uiText.entityCreateSuccessTitle,
      uiText.entityCreateSuccessMessage
        .replace("{name}", payload.clientName)
        .replace("{language}", selectedLanguage)
    );
  };

  const handleClientSignup = async (payload) => {
    try {
      const data = await signupClient(payload);
      openSystemMessage("success", uiText.signupModal.ok_signup_title, uiText.signupModal.ok_signup_message);
      return data;
    } catch (error) {
      openSystemMessage("error", uiText.signupModal.title, error.message);
      throw error;
    }
  };

  const handleClientSignupConfirm = async (payload) => {
    try {
      await confirmClientSignup(payload);
      openSystemMessage("success", uiText.signupModal.ok_confirm_title, uiText.signupModal.ok_confirm_message);
    } catch (error) {
      openSystemMessage("error", uiText.signupModal.title, error.message);
      throw error;
    }
  };

  const handlePasswordResetRequest = async (payload) => {
    const data = await requestPasswordReset(payload);
    openSystemMessage("success", uiText.loginModal.reset_request_ok_title, uiText.loginModal.reset_request_ok_message);
    return data;
  };

  const handlePasswordResetConfirm = async (payload) => {
    await confirmPasswordReset(payload);
    openSystemMessage("success", uiText.loginModal.reset_ok_title, uiText.loginModal.reset_ok_message);
  };

  const handleLoginSubmit = async (payload) => {
    try {
      const data = await loginClient(payload);
      if (data?.mfa_required) {
        return data;
      }
      const ctx = {
        ...(data?.auth_context || {}),
        ...(data?.profile || {}),
        ...(data?.session || {}),
      };
      setStoredAuthContext(ctx);
      setAuthContext(ctx);
      return data;
    } catch (error) {
      openSystemMessage("error", uiText.loginModal.fail_title, error.message);
      throw error;
    }
  };

  const handleVerifyLoginMfa = async (payload) => {
    try {
      const data = await verifyLoginMfa(payload);
      const ctx = {
        ...(data?.auth_context || {}),
        ...(data?.profile || {}),
        ...(data?.session || {}),
      };
      setStoredAuthContext(ctx);
      setAuthContext(ctx);
      return data;
    } catch (error) {
      openSystemMessage("error", uiText.loginModal.fail_title, error.message);
      throw error;
    }
  };

  const handleIncidentSubmit = async (payload) => {
    try {
      const data = await createIncident(payload);
      setIsIncidentModalOpen(false);
      setIncidentReloadSignal((prev) => prev + 1);
      openSystemMessage(
        "success",
        "Incidente aberto",
        `Incidente #${data.incident_id} criado com SLA ${data.sla_due_at}.`
      );
    } catch (error) {
      openSystemMessage("error", "Falha ao abrir incidente", error.message);
    }
  };

  const handleIncidentStatusSubmit = async (payload) => {
    try {
      const data = await updateIncidentStatus(payload);
      setIsIncidentStatusModalOpen(false);
      setSelectedIncident(null);
      setIncidentReloadSignal((prev) => prev + 1);
      openSystemMessage(
        "success",
        "Status atualizado",
        `Incidente #${data.incident.incident_id} atualizado para ${data.incident.status}.`
      );
    } catch (error) {
      openSystemMessage("error", "Falha ao atualizar status", error.message);
    }
  };

  const handleQuickIncidentStatusUpdate = async (incidentId, newStatus) => {
    try {
      const data = await updateIncidentStatus({ incident_id: incidentId, new_status: newStatus });
      setIncidentReloadSignal((prev) => prev + 1);
      openSystemMessage(
        "success",
        "Status atualizado",
        `Incidente #${data.incident.incident_id} atualizado para ${data.incident.status}.`
      );
    } catch (error) {
      openSystemMessage("error", "Falha ao atualizar status", error.message);
    }
  };

  const renderActivePage = () => {
    const panels = {
      onboarding: <OnboardingPanel onSystemMessage={openSystemMessage} />,
      inventario: <InventoryPanel onSystemMessage={openSystemMessage} />,
      sugestoes: <SuggestionsPanel onSystemMessage={openSystemMessage} />,
      "chat-bi": <ChatBiPanel onSystemMessage={openSystemMessage} />,
      eventos: <EventsPanel onSystemMessage={openSystemMessage} />,
      operacao: <OperationPanel onSystemMessage={openSystemMessage} />,
      faturamento: <BillingPanel onSystemMessage={openSystemMessage} />,
      auditoria: <AuditPanel onSystemMessage={openSystemMessage} />,
      "seguranca-sql": <SqlSecurityPanel onSystemMessage={openSystemMessage} />,
      acesso: <AccessPanel onSystemMessage={openSystemMessage} />,
      configuracao: (
        <ConfiguracaoPanel
          onSystemMessage={openSystemMessage}
          onNavigate={setActivePage}
          onPreferenceApplied={(preference) => {
            setUiLanguage(preference?.language_code || "pt-BR");
            setUserTheme(preference?.theme_code || "light");
          }}
          onSessionRevokedCurrent={() => performLogout()}
        />
      ),
      lgpd: <LgpdPanel onSystemMessage={openSystemMessage} />,
      incidentes: (
        <IncidentPanel
          onOpenCreate={() => setIsIncidentModalOpen(true)}
          onOpenStatusModal={(incident) => {
            setSelectedIncident(incident);
            setIsIncidentStatusModalOpen(true);
          }}
          onQuickStatusUpdate={handleQuickIncidentStatusUpdate}
          onSystemMessage={openSystemMessage}
          reloadSignal={incidentReloadSignal}
        />
      ),
    };

    return panels[activePage] || (
      <PagePanel
        title={activeLabel}
        subtitle={localizedSubtitleByPage[activePage]}
        onOpenCreate={() => setIsEntityModalOpen(true)}
        onShowAlert={() => openSystemMessage("warning", uiText.genericAlertTitle, uiText.genericAlertMessage)}
      />
    );
  };

  if (!authContext) {
    return (
      <>
        <AuthScreen
          labels={{
            signIn: uiText.signIn,
            signUp: uiText.signUp,
            login: uiText.loginModal,
            signup: uiText.signupModal,
            auth: uiText.authScreen,
          }}
          onLogin={handleLoginSubmit}
          onVerifyMfa={handleVerifyLoginMfa}
          onSignup={handleClientSignup}
          onConfirm={handleClientSignupConfirm}
          onPasswordResetRequest={handlePasswordResetRequest}
          onPasswordResetConfirm={handlePasswordResetConfirm}
        />
        <SystemMessageModal
          state={messageModal}
          onClose={() => setMessageModal((prev) => ({ ...prev, open: false }))}
        />
      </>
    );
  }

  return (
    <div className="app-shell">
      <SideMenu
        items={localizedNavItems}
        activeKey={activePage}
        onSelect={setActivePage}
        logoutLabel={uiText.signOut}
        onLogout={() => performLogout()}
      />

      <main className="content-area">
        <div className="page-actions">
          <span className="chip">
            {uiText.sessionLabel
              .replace("{email}", authContext.email || "-")
              .replace("{role}", authContext.role || "viewer")}
          </span>
          {!isGlobalSuperadmin && (
            <button type="button" className="btn btn-secondary btn-small" onClick={() => setIsSetupAssistantOpen(true)}>
              {uiText.setupAssistant}
            </button>
          )}
          {!isGlobalSuperadmin && (
            <button
              type="button"
              className="btn btn-secondary btn-small"
              onClick={() => {
                if (pendingSetupStep) setActivePage(pendingSetupStep.targetPage);
              }}
              disabled={!pendingSetupStep}
            >
              {uiText.setupWizard.nextPending}
            </button>
          )}
          {!isGlobalSuperadmin && currentSetupStep && !effectiveCompletedSetupSteps.includes(currentSetupStep.key) && (
            <button
              type="button"
              className="btn btn-primary btn-small"
              onClick={() => completeSetupStep(currentSetupStep.key)}
            >
              {uiText.setupWizard.completeCurrent}
            </button>
          )}
          {!isGlobalSuperadmin && (
            <button
              type="button"
              className="btn btn-secondary btn-small"
              onClick={() =>
                setMiniChecklistCollapsed((prev) => {
                  const next = !prev;
                  window.localStorage.setItem(SETUP_MINI_CHECKLIST_COLLAPSED_KEY, next ? "1" : "0");
                  return next;
                })
              }
            >
              {miniChecklistCollapsed ? uiText.setupWizard.expand : uiText.setupWizard.collapse}
            </button>
          )}
        </div>
        {!isGlobalSuperadmin && (
          <div className="setup-status-summary">
            <span className="chip chip-step-done">
              {uiText.setupWizard.statusSummary.done.replace("{count}", String(setupStatusCounts.done))}
            </span>
            <span className="chip chip-step-partial">
              {uiText.setupWizard.statusSummary.partial.replace("{count}", String(setupStatusCounts.partial))}
            </span>
            <span className="chip chip-step-blocked">
              {uiText.setupWizard.statusSummary.blocked.replace("{count}", String(setupStatusCounts.blocked))}
            </span>
          </div>
        )}
        {!isGlobalSuperadmin && (
          <div className={`setup-mini-checklist-wrap ${miniChecklistCollapsed ? "collapsed" : ""}`} aria-hidden={miniChecklistCollapsed}>
            <div className="setup-mini-checklist" role="navigation" aria-label={uiText.setupAssistant}>
            {ASSISTANT_STEPS.map((step, index) => {
              const status = setupStepStatusByKey[step.key] || "pending";
              const label = uiText.setupWizard.steps?.[step.key]?.title || step.title;
              const reason = setupPendingReasons[step.key] || "";
              const tooltipReason = reason ? uiText.setupWizard.reasonLabel.replace("{reason}", reason) : "";
              const statusText =
                status === "done"
                  ? uiText.setupWizard.done
                  : status === "partial"
                  ? uiText.setupWizard.partial
                  : status === "blocked"
                  ? uiText.setupWizard.blocked
                  : uiText.setupWizard.pending;
              return (
                <button
                  key={step.key}
                  type="button"
                  className={`mini-step mini-step-${status} ${activePage === step.targetPage ? "active" : ""}`}
                  onClick={() => setActivePage(step.targetPage)}
                  title={tooltipReason}
                >
                  <span className="mini-step-index">{index + 1}</span>
                  <span className="mini-step-text">{label}</span>
                  <span className="mini-step-status">{statusText}</span>
                </button>
                );
              })}
            </div>
          </div>
        )}

        {renderActivePage()}
      </main>

      <EntityFormModal
        open={isEntityModalOpen}
        title={`${uiText.entityCreateTitlePrefix} - ${activeLabel}`}
        defaultLanguage={uiLanguage}
        labels={uiText.entityForm}
        languageOptions={[
          { value: "pt-BR", label: "Portugues (Brasil)" },
          { value: "en-US", label: "English (US)" },
          { value: "es-ES", label: "Espanol" },
        ]}
        onClose={() => setIsEntityModalOpen(false)}
        onSubmit={handleEntityFormSubmit}
      />

      <IncidentFormModal
        open={isIncidentModalOpen}
        onClose={() => setIsIncidentModalOpen(false)}
        onSubmit={handleIncidentSubmit}
      />

      <IncidentStatusModal
        open={isIncidentStatusModalOpen}
        incident={selectedIncident}
        onClose={() => {
          setIsIncidentStatusModalOpen(false);
          setSelectedIncident(null);
        }}
        onSubmit={handleIncidentStatusSubmit}
      />

      <SystemMessageModal
        state={messageModal}
        onClose={() => setMessageModal((prev) => ({ ...prev, open: false }))}
      />

      <SetupAssistantModal
        open={isSetupAssistantOpen}
        onClose={() => setIsSetupAssistantOpen(false)}
        onSkipAll={postponeAssistant}
        onCompleteStep={completeSetupStep}
        onContinue={() => {
          if (pendingSetupStep) {
            setActivePage(pendingSetupStep.targetPage);
          }
          setIsSetupAssistantOpen(false);
        }}
        completedStepKeys={effectiveCompletedSetupSteps}
        pendingReasonByStep={setupPendingReasons}
        stepStatusByKey={setupStepStatusByKey}
        uiText={uiText.setupWizard}
        onGoToStep={(pageKey) => {
          setActivePage(pageKey);
          setIsSetupAssistantOpen(false);
        }}
      />
    </div>
  );
}

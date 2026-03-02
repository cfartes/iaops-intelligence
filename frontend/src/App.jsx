import { useEffect, useMemo, useState } from "react";
import SideMenu from "./components/SideMenu";
import EntityFormModal from "./components/EntityFormModal";
import SystemMessageModal from "./components/SystemMessageModal";
import IncidentFormModal from "./components/IncidentFormModal";
import IncidentStatusModal from "./components/IncidentStatusModal";
import SetupAssistantModal, { ASSISTANT_STEPS } from "./components/SetupAssistantModal";
import PagePanel from "./pages/PagePanel";
import OnboardingPanel from "./pages/OnboardingPanel";
import InventoryPanel from "./pages/InventoryPanel";
import IncidentPanel from "./pages/IncidentPanel";
import EventsPanel from "./pages/EventsPanel";
import OperationPanel from "./pages/OperationPanel";
import AuditPanel from "./pages/AuditPanel";
import SqlSecurityPanel from "./pages/SqlSecurityPanel";
import ChatBiPanel from "./pages/ChatBiPanel";
import AccessPanel from "./pages/AccessPanel";
import ConfiguracaoPanel from "./pages/ConfiguracaoPanel";
import { NAV_ITEMS } from "./state/nav";
import {
  createIncident,
  getMfaStatus,
  getOperationHealth,
  getTenantLlmConfig,
  getUserTenantPreference,
  listAccessUsers,
  listOnboardingMonitoredColumns,
  listOnboardingMonitoredTables,
  listTenantDataSources,
  updateIncidentStatus,
  updateUserTenantPreference,
} from "./api/mcpApi";

const SETUP_PROGRESS_STORAGE_KEY = "iaops_setup_assistant_progress_v1";
const SETUP_DEFER_UNTIL_STORAGE_KEY = "iaops_setup_assistant_defer_until_v1";
const SETUP_MINI_CHECKLIST_COLLAPSED_KEY = "iaops_setup_mini_checklist_collapsed_v1";
const SETUP_DEFER_HOURS = 24;

const UI_TEXT = {
  pt: {
    nav: {
      onboarding: "Onboarding",
      inventario: "Inventario",
      sugestoes: "Sugestoes",
      "chat-bi": "Chat BI",
      eventos: "Eventos",
      incidentes: "Incidentes",
      auditoria: "Auditoria",
      "seguranca-sql": "Seguranca SQL",
      acesso: "Acesso (Owner)",
      lgpd: "LGPD",
      operacao: "Operacao",
      faturamento: "Faturamento (Owner)",
      parcelas: "Parcelas (Owner)",
      configuracao: "Configuracao",
    },
    subtitles: {
      onboarding: "Configure cliente, tenant e fonte de dados com fluxo guiado.",
      inventario: "Explore tabelas, colunas e classificacao de metadados por tenant.",
      sugestoes: "Receba recomendacoes de classificacao e descricao para governanca.",
      "chat-bi": "Perguntas em linguagem natural com contexto de metadados e politicas LGPD.",
      eventos: "Acompanhe mudancas estruturais detectadas e alertas por severidade.",
      incidentes: "Gerencie ciclo de vida de incidentes com SLA e rastreabilidade.",
      auditoria: "Consulte trilhas de auditoria das operacoes criticas do sistema.",
      "seguranca-sql": "Defina guardrails e politicas de seguranca para consultas.",
      acesso: "Controle usuarios, papeis e restricoes de privilegio por tenant.",
      lgpd: "Administre politicas, mascaramento e solicitacoes de titulares.",
      operacao: "Visualize saude operacional e status de integracoes.",
      faturamento: "Gerencie planos, assinaturas e limites por cliente.",
      parcelas: "Acompanhe vencimentos, baixas e inadimplencia.",
      configuracao: "Ajuste preferencias de tenant, notificacoes e LLM.",
    },
    genericModule: "Modulo",
    setupAssistant: "Assistente Inicial",
    genericAlertTitle: "Alerta de Governanca",
    genericAlertMessage: "Este tenant possui configuracoes pendentes de LGPD e deve ser revisado.",
    entityCreateTitlePrefix: "Novo cadastro",
    setupWizard: {
      title: "Assistente Inicial de Configuracao",
      intro: "Use as etapas abaixo para configurar o app no primeiro acesso. Voce pode pular qualquer etapa.",
      progress: "Progresso: {completed}/{total} etapas concluidas.",
      pending: "Pendente",
      done: "Concluida",
      partial: "Parcial",
      blocked: "Bloqueada",
      completed: "Concluida",
      goToStep: "Ir para etapa",
      markDone: "Marcar como concluida",
      completeCurrent: "Concluir etapa atual",
      continue: "Continuar de onde parei",
      collapse: "Recolher trilha",
      expand: "Expandir trilha",
      close: "Fechar",
      postpone: "Lembrar mais tarde",
      stepDoneTitle: "Etapa concluida",
      stepDoneMessage: "Etapa marcada como concluida.",
      allDoneTitle: "Setup concluido",
      allDoneMessage: "Todas as etapas do assistente foram concluidas.",
      reasonLabel: "Pendencia: {reason}",
      reasons: {
        onboarding_no_source: "Cadastre pelo menos uma fonte de dados no tenant.",
        onboarding_no_table: "Cadastre ao menos uma tabela monitorada.",
        onboarding_no_column: "Cadastre ao menos uma coluna monitorada.",
        acesso_no_users: "Cadastre pelo menos um usuario.",
        configuracao_missing: "Configure LLM do tenant/app ou ative MFA.",
        operacao_missing_channels: "Configure ao menos um canal de notificacao.",
      },
      steps: {
        onboarding: {
          title: "Configurar Tenant e Fontes",
          description: "Cadastre tenant, fontes de dados, tabelas e colunas monitoradas.",
        },
        acesso: {
          title: "Cadastrar Usuarios e Acesso",
          description: "Defina usuarios, roles e reset MFA quando necessario.",
        },
        configuracao: {
          title: "Configurar LLM e Preferencias",
          description: "Defina LLM do tenant (ou LLM do app), MFA e preferencias iniciais.",
        },
        operacao: {
          title: "Configurar Canais e Alertas",
          description: "Valide notificacoes Telegram/WhatsApp e operacao.",
        },
      },
    },
    entityForm: {
      clientName: "Nome Fantasia",
      legalName: "Razao Social",
      cnpj: "CNPJ",
      contactEmail: "E-mail Contato",
      language: "Idioma",
      cancel: "Cancelar",
      save: "Salvar",
    },
    entityCreateSuccessTitle: "Cadastro confirmado",
    entityCreateSuccessMessage: "Cadastro recebido para {name}. Idioma inicial: {language}.",
  },
  en: {
    nav: {
      onboarding: "Onboarding",
      inventario: "Inventory",
      sugestoes: "Suggestions",
      "chat-bi": "Chat BI",
      eventos: "Events",
      incidentes: "Incidents",
      auditoria: "Audit",
      "seguranca-sql": "SQL Security",
      acesso: "Access (Owner)",
      lgpd: "LGPD",
      operacao: "Operations",
      faturamento: "Billing (Owner)",
      parcelas: "Installments (Owner)",
      configuracao: "Configuration",
    },
    subtitles: {
      onboarding: "Configure client, tenant and data source with a guided flow.",
      inventario: "Explore tables, columns and metadata classification by tenant.",
      sugestoes: "Receive governance suggestions for classification and description.",
      "chat-bi": "Natural language questions with metadata context and LGPD policies.",
      eventos: "Track structural changes and alerts by severity.",
      incidentes: "Manage incident lifecycle with SLA and traceability.",
      auditoria: "Review audit trails for critical system operations.",
      "seguranca-sql": "Define SQL guardrails and query safety policies.",
      acesso: "Manage users, roles and privilege restrictions per tenant.",
      lgpd: "Manage policies, masking and data subject requests.",
      operacao: "View operational health and integration status.",
      faturamento: "Manage plans, subscriptions and client limits.",
      parcelas: "Track due dates, payments and delinquency.",
      configuracao: "Adjust tenant preferences, notifications and LLM.",
    },
    genericModule: "Module",
    setupAssistant: "Initial Assistant",
    genericAlertTitle: "Governance Alert",
    genericAlertMessage: "This tenant has pending LGPD settings and must be reviewed.",
    entityCreateTitlePrefix: "New record",
    setupWizard: {
      title: "Initial Setup Assistant",
      intro: "Use the steps below to configure the app on first access. You can skip any step.",
      progress: "Progress: {completed}/{total} steps completed.",
      pending: "Pending",
      done: "Done",
      partial: "Partial",
      blocked: "Blocked",
      completed: "Completed",
      goToStep: "Go to step",
      markDone: "Mark as done",
      completeCurrent: "Complete current step",
      continue: "Continue where I left off",
      collapse: "Collapse tracker",
      expand: "Expand tracker",
      close: "Close",
      postpone: "Remind later",
      stepDoneTitle: "Step completed",
      stepDoneMessage: "Step marked as completed.",
      allDoneTitle: "Setup completed",
      allDoneMessage: "All setup assistant steps are completed.",
      reasonLabel: "Pending: {reason}",
      reasons: {
        onboarding_no_source: "Register at least one tenant data source.",
        onboarding_no_table: "Register at least one monitored table.",
        onboarding_no_column: "Register at least one monitored column.",
        acesso_no_users: "Register at least one user.",
        configuracao_missing: "Configure tenant/app LLM or enable MFA.",
        operacao_missing_channels: "Configure at least one notification channel.",
      },
      steps: {
        onboarding: {
          title: "Configure Tenant and Sources",
          description: "Register tenant, data sources, monitored tables and columns.",
        },
        acesso: {
          title: "Configure Users and Access",
          description: "Define users, roles and MFA reset policy when needed.",
        },
        configuracao: {
          title: "Configure LLM and Preferences",
          description: "Set tenant LLM (or app default), MFA and initial preferences.",
        },
        operacao: {
          title: "Configure Channels and Alerts",
          description: "Validate Telegram/WhatsApp notifications and operations.",
        },
      },
    },
    entityForm: {
      clientName: "Trade Name",
      legalName: "Legal Name",
      cnpj: "Tax ID (CNPJ)",
      contactEmail: "Contact Email",
      language: "Language",
      cancel: "Cancel",
      save: "Save",
    },
    entityCreateSuccessTitle: "Registration confirmed",
    entityCreateSuccessMessage: "Registration received for {name}. Initial language: {language}.",
  },
  es: {
    nav: {
      onboarding: "Onboarding",
      inventario: "Inventario",
      sugestoes: "Sugerencias",
      "chat-bi": "Chat BI",
      eventos: "Eventos",
      incidentes: "Incidentes",
      auditoria: "Auditoria",
      "seguranca-sql": "Seguridad SQL",
      acesso: "Acceso (Owner)",
      lgpd: "LGPD",
      operacao: "Operacion",
      faturamento: "Facturacion (Owner)",
      parcelas: "Cuotas (Owner)",
      configuracao: "Configuracion",
    },
    subtitles: {
      onboarding: "Configure cliente, tenant y fuente de datos con flujo guiado.",
      inventario: "Explore tablas, columnas y clasificacion de metadatos por tenant.",
      sugestoes: "Reciba sugerencias de clasificacion y descripcion para gobernanza.",
      "chat-bi": "Preguntas en lenguaje natural con contexto de metadatos y politicas LGPD.",
      eventos: "Acompanhe cambios estructurales y alertas por severidad.",
      incidentes: "Gestione el ciclo de vida de incidentes con SLA y trazabilidad.",
      auditoria: "Consulte trazas de auditoria de operaciones criticas.",
      "seguranca-sql": "Defina guardrails y politicas de seguridad para consultas.",
      acesso: "Controle usuarios, roles y restricciones por tenant.",
      lgpd: "Administre politicas, enmascaramiento y solicitudes de titulares.",
      operacao: "Visualice salud operacional y estado de integraciones.",
      faturamento: "Gestione planes, suscripciones y limites por cliente.",
      parcelas: "Acompanhe vencimientos, pagos y morosidad.",
      configuracao: "Ajuste preferencias de tenant, notificaciones y LLM.",
    },
    genericModule: "Modulo",
    setupAssistant: "Asistente Inicial",
    genericAlertTitle: "Alerta de Gobernanza",
    genericAlertMessage: "Este tenant tiene configuraciones LGPD pendientes y debe revisarse.",
    entityCreateTitlePrefix: "Nuevo registro",
    setupWizard: {
      title: "Asistente Inicial de Configuracion",
      intro: "Use las etapas abajo para configurar la app en el primer acceso. Puede omitir cualquier etapa.",
      progress: "Progreso: {completed}/{total} etapas completadas.",
      pending: "Pendiente",
      done: "Completada",
      partial: "Parcial",
      blocked: "Bloqueada",
      completed: "Completada",
      goToStep: "Ir a etapa",
      markDone: "Marcar como completada",
      completeCurrent: "Completar etapa actual",
      continue: "Continuar donde lo deje",
      collapse: "Contraer seguimiento",
      expand: "Expandir seguimiento",
      close: "Cerrar",
      postpone: "Recordar despues",
      stepDoneTitle: "Etapa completada",
      stepDoneMessage: "Etapa marcada como completada.",
      allDoneTitle: "Setup completado",
      allDoneMessage: "Todas las etapas del asistente fueron completadas.",
      reasonLabel: "Pendiente: {reason}",
      reasons: {
        onboarding_no_source: "Registre al menos una fuente de datos del tenant.",
        onboarding_no_table: "Registre al menos una tabla monitoreada.",
        onboarding_no_column: "Registre al menos una columna monitoreada.",
        acesso_no_users: "Registre al menos un usuario.",
        configuracao_missing: "Configure LLM del tenant/app o habilite MFA.",
        operacao_missing_channels: "Configure al menos un canal de notificacion.",
      },
      steps: {
        onboarding: {
          title: "Configurar Tenant y Fuentes",
          description: "Registre tenant, fuentes de datos, tablas y columnas monitoreadas.",
        },
        acesso: {
          title: "Configurar Usuarios y Acceso",
          description: "Defina usuarios, roles y reset de MFA cuando sea necesario.",
        },
        configuracao: {
          title: "Configurar LLM y Preferencias",
          description: "Defina LLM del tenant (o LLM de la app), MFA y preferencias iniciales.",
        },
        operacao: {
          title: "Configurar Canales y Alertas",
          description: "Valide notificaciones Telegram/WhatsApp y operacion.",
        },
      },
    },
    entityForm: {
      clientName: "Nombre Comercial",
      legalName: "Razon Social",
      cnpj: "CNPJ",
      contactEmail: "Correo de Contacto",
      language: "Idioma",
      cancel: "Cancelar",
      save: "Guardar",
    },
    entityCreateSuccessTitle: "Registro confirmado",
    entityCreateSuccessMessage: "Registro recibido para {name}. Idioma inicial: {language}.",
  },
};

export default function App() {
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
  const localizedNavItems = useMemo(
    () =>
      NAV_ITEMS.map((item) => ({
        ...item,
        label: uiText.nav[item.key] || item.label,
      })),
    [uiText]
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
        (reason === uiText.setupWizard.reasons.onboarding_no_table ||
          reason === uiText.setupWizard.reasons.onboarding_no_column);
      status[step.key] = isPartial ? "partial" : reason ? "blocked" : "pending";
    }
    return status;
  }, [effectiveCompletedSetupSteps, setupPendingReasons, uiText]);

  const openSystemMessage = (tone, title, message) => {
    setMessageModal({ open: true, tone, title, message });
  };

  useEffect(() => {
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
    evaluateSetupProgress();
    loadUserPreference();
  }, []);

  useEffect(() => {
    if (isSetupAssistantOpen) {
      evaluateSetupProgress();
    }
  }, [isSetupAssistantOpen]);

  useEffect(() => {
    document.documentElement.setAttribute("lang", uiLanguage || "pt-BR");
  }, [uiLanguage]);

  useEffect(() => {
    document.documentElement.dataset.theme = userTheme || "light";
  }, [userTheme]);

  const loadUserPreference = async () => {
    try {
      const data = await getUserTenantPreference();
      const preference = data.preference || {};
      setUiLanguage(preference.language_code || "pt-BR");
      setUserTheme(preference.theme_code || "light");
    } catch (error) {
      openSystemMessage("warning", "Preferencias padrao", "Nao foi possivel carregar preferencias do usuario+tenant.");
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
          const columnsData = await listOnboardingMonitoredColumns();
          const columns = columnsData?.columns || [];
          if (columns.length === 0) {
            reasons.onboarding = reasonText.onboarding_no_column;
          } else {
            steps.push("onboarding");
          }
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

  return (
    <div className="app-shell">
      <SideMenu items={localizedNavItems} activeKey={activePage} onSelect={setActivePage} />

      <main className="content-area">
        <div className="page-actions">
          <button type="button" className="btn btn-secondary btn-small" onClick={() => setIsSetupAssistantOpen(true)}>
            {uiText.setupAssistant}
          </button>
          {currentSetupStep && !effectiveCompletedSetupSteps.includes(currentSetupStep.key) && (
            <button
              type="button"
              className="btn btn-primary btn-small"
              onClick={() => completeSetupStep(currentSetupStep.key)}
            >
              {uiText.setupWizard.completeCurrent}
            </button>
          )}
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
        </div>
        <div className={`setup-mini-checklist-wrap ${miniChecklistCollapsed ? "collapsed" : ""}`} aria-hidden={miniChecklistCollapsed}>
          <div className="setup-mini-checklist" role="navigation" aria-label={uiText.setupAssistant}>
            {ASSISTANT_STEPS.map((step, index) => {
              const status = setupStepStatusByKey[step.key] || "pending";
              const label = uiText.setupWizard.steps?.[step.key]?.title || step.title;
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
                >
                  <span className="mini-step-index">{index + 1}</span>
                  <span className="mini-step-text">{label}</span>
                  <span className="mini-step-status">{statusText}</span>
                </button>
              );
            })}
          </div>
        </div>

        {activePage === "onboarding" ? (
          <OnboardingPanel onSystemMessage={openSystemMessage} />
        ) : activePage === "inventario" ? (
          <InventoryPanel onSystemMessage={openSystemMessage} />
        ) : activePage === "chat-bi" ? (
          <ChatBiPanel onSystemMessage={openSystemMessage} />
        ) : activePage === "eventos" ? (
          <EventsPanel onSystemMessage={openSystemMessage} />
        ) : activePage === "operacao" ? (
          <OperationPanel onSystemMessage={openSystemMessage} />
        ) : activePage === "auditoria" ? (
          <AuditPanel onSystemMessage={openSystemMessage} />
        ) : activePage === "seguranca-sql" ? (
          <SqlSecurityPanel onSystemMessage={openSystemMessage} />
        ) : activePage === "acesso" ? (
          <AccessPanel onSystemMessage={openSystemMessage} />
        ) : activePage === "configuracao" ? (
          <ConfiguracaoPanel
            onSystemMessage={openSystemMessage}
            onPreferenceApplied={(preference) => {
              setUiLanguage(preference?.language_code || "pt-BR");
              setUserTheme(preference?.theme_code || "light");
            }}
          />
        ) : activePage === "incidentes" ? (
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
        ) : (
          <PagePanel
            title={activeLabel}
            subtitle={localizedSubtitleByPage[activePage]}
            onOpenCreate={() => setIsEntityModalOpen(true)}
            onShowAlert={() =>
              openSystemMessage(
                "warning",
                uiText.genericAlertTitle,
                uiText.genericAlertMessage
              )
            }
          />
        )}
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

import { useEffect, useMemo, useState } from "react";
import SideMenu from "./components/SideMenu";
import EntityFormModal from "./components/EntityFormModal";
import SystemMessageModal from "./components/SystemMessageModal";
import IncidentFormModal from "./components/IncidentFormModal";
import IncidentStatusModal from "./components/IncidentStatusModal";
import SetupAssistantModal from "./components/SetupAssistantModal";
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
import { createIncident, getUserTenantPreference, updateIncidentStatus } from "./api/mcpApi";

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
  },
};

export default function App() {
  const [activePage, setActivePage] = useState("onboarding");
  const [userTheme, setUserTheme] = useState("light");
  const [uiLanguage, setUiLanguage] = useState("pt-BR");
  const [isSetupAssistantOpen, setIsSetupAssistantOpen] = useState(false);
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

  const openSystemMessage = (tone, title, message) => {
    setMessageModal({ open: true, tone, title, message });
  };

  useEffect(() => {
    const dismissed = window.localStorage.getItem("iaops_setup_assistant_dismissed");
    if (!dismissed) {
      setIsSetupAssistantOpen(true);
    }
    loadUserPreference();
  }, []);

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

  const dismissAssistant = () => {
    window.localStorage.setItem("iaops_setup_assistant_dismissed", "1");
    setIsSetupAssistantOpen(false);
  };

  const handleEntityFormSubmit = (payload) => {
    setIsEntityModalOpen(false);
    openSystemMessage("success", "Cadastro confirmado", `Cadastro recebido para ${payload.clientName}.`);
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
        onSkipAll={dismissAssistant}
        onGoToStep={(pageKey) => {
          setActivePage(pageKey);
          setIsSetupAssistantOpen(false);
        }}
      />
    </div>
  );
}

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
import { createIncident, updateIncidentStatus } from "./api/mcpApi";

const SUBTITLE_BY_PAGE = {
  onboarding: "Configure cliente, tenant e fonte de dados com fluxo guiado.",
  inventario: "Explore tabelas, colunas e classificacao de metadados por tenant.",
  sugestoes: "Receba recomendacoes de classificacao e descricao para governanca.",
  "chat-bi": "Perguntas em linguagem natural com SQL assistido e politicas LGPD.",
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
};

export default function App() {
  const [activePage, setActivePage] = useState("onboarding");
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

  const activeLabel = useMemo(
    () => NAV_ITEMS.find((item) => item.key === activePage)?.label || "Modulo",
    [activePage]
  );

  const openSystemMessage = (tone, title, message) => {
    setMessageModal({ open: true, tone, title, message });
  };

  useEffect(() => {
    const dismissed = window.localStorage.getItem("iaops_setup_assistant_dismissed");
    if (!dismissed) {
      setIsSetupAssistantOpen(true);
    }
  }, []);

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
      <SideMenu items={NAV_ITEMS} activeKey={activePage} onSelect={setActivePage} />

      <main className="content-area">
        <div className="page-actions">
          <button type="button" className="btn btn-secondary btn-small" onClick={() => setIsSetupAssistantOpen(true)}>
            Assistente Inicial
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
          <ConfiguracaoPanel onSystemMessage={openSystemMessage} />
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
            subtitle={SUBTITLE_BY_PAGE[activePage]}
            onOpenCreate={() => setIsEntityModalOpen(true)}
            onShowAlert={() =>
              openSystemMessage(
                "warning",
                "Alerta de Governanca",
                "Este tenant possui configuracoes pendentes de LGPD e deve ser revisado."
              )
            }
          />
        )}
      </main>

      <EntityFormModal
        open={isEntityModalOpen}
        title={`Novo cadastro - ${activeLabel}`}
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

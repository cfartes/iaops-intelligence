const ASSISTANT_STEPS = [
  {
    key: "onboarding",
    title: "Configurar Tenant e Fontes",
    description: "Cadastre tenant, fontes de dados, tabelas e colunas monitoradas.",
    targetPage: "onboarding",
  },
  {
    key: "acesso",
    title: "Cadastrar Usuarios e Acesso",
    description: "Defina usuarios, roles e reset MFA quando necessario.",
    targetPage: "acesso",
  },
  {
    key: "configuracao",
    title: "Configurar LLM e Preferencias",
    description: "Defina LLM do tenant (ou LLM do app), MFA e preferencias iniciais.",
    targetPage: "configuracao",
  },
  {
    key: "operacao",
    title: "Configurar Canais e Alertas",
    description: "Valide notificacoes Telegram/WhatsApp e operacao.",
    targetPage: "operacao",
  },
];

export default function SetupAssistantModal({ open, onClose, onSkipAll, onGoToStep }) {
  if (!open) return null;

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Assistente Inicial de Configuracao</h3>
        </header>
        <section className="modal-content">
          <p>Use as etapas abaixo para configurar o app no primeiro acesso. Voce pode pular qualquer etapa.</p>
          <div className="data-list">
            {ASSISTANT_STEPS.map((step, index) => (
              <div key={step.key} className="row-card">
                <div>
                  <strong>
                    {index + 1}. {step.title}
                  </strong>
                  <p className="muted">{step.description}</p>
                </div>
                <button type="button" className="btn btn-secondary btn-small" onClick={() => onGoToStep(step.targetPage)}>
                  Ir para etapa
                </button>
              </div>
            ))}
          </div>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              Fechar
            </button>
            <button type="button" className="btn btn-primary" onClick={onSkipAll}>
              Pular por enquanto
            </button>
          </div>
        </section>
      </div>
    </div>
  );
}

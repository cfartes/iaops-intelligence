export const ASSISTANT_STEPS = [
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

export default function SetupAssistantModal({
  open,
  onClose,
  onSkipAll,
  onGoToStep,
  onCompleteStep,
  onContinue,
  completedStepKeys = [],
  pendingReasonByStep = {},
  stepStatusByKey = {},
  uiText,
}) {
  if (!open) return null;

  const completedSet = new Set(completedStepKeys);
  const total = ASSISTANT_STEPS.length;
  const completed = ASSISTANT_STEPS.filter((step) => completedSet.has(step.key)).length;
  const progressPct = Math.round((completed / total) * 100);

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{uiText?.title || "Assistente Inicial de Configuracao"}</h3>
        </header>
        <section className="modal-content">
          <p>{uiText?.intro || "Use as etapas abaixo para configurar o app no primeiro acesso. Voce pode pular qualquer etapa."}</p>
          <p className="muted">
            {(uiText?.progress || "Progresso: {completed}/{total} etapas concluídas.")
              .replace("{completed}", String(completed))
              .replace("{total}", String(total))}
          </p>
          <progress value={completed} max={total} aria-label={`${progressPct}%`} style={{ width: "100%" }} />
          <div className="data-list">
            {ASSISTANT_STEPS.map((step, index) => (
              <div key={step.key} className={`row-card setup-step setup-step-${stepStatusByKey[step.key] || "pending"}`}>
                <div>
                  <strong>
                    {index + 1}. {uiText?.steps?.[step.key]?.title || step.title}
                  </strong>
                  <p className="muted">{uiText?.steps?.[step.key]?.description || step.description}</p>
                  {!completedSet.has(step.key) && pendingReasonByStep[step.key] && (
                    <p className="muted">{(uiText?.reasonLabel || "Pendencia: {reason}").replace("{reason}", pendingReasonByStep[step.key])}</p>
                  )}
                </div>
                <div className="chip-row">
                  <span className={`chip chip-step-${stepStatusByKey[step.key] || "pending"}`}>
                    {stepStatusByKey[step.key] === "done"
                      ? uiText?.done || "Concluida"
                      : stepStatusByKey[step.key] === "partial"
                      ? uiText?.partial || "Parcial"
                      : stepStatusByKey[step.key] === "blocked"
                      ? uiText?.blocked || "Bloqueada"
                      : uiText?.pending || "Pendente"}
                  </span>
                  <button type="button" className="btn btn-secondary btn-small" onClick={() => onGoToStep(step.targetPage)}>
                    {uiText?.goToStep || "Ir para etapa"}
                  </button>
                  <button
                    type="button"
                    className="btn btn-primary btn-small"
                    onClick={() => onCompleteStep?.(step.key)}
                    disabled={completedSet.has(step.key)}
                  >
                    {completedSet.has(step.key) ? uiText?.completed || "Concluida" : uiText?.markDone || "Marcar como concluida"}
                  </button>
                </div>
              </div>
            ))}
          </div>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onContinue}>
              {uiText?.continue || "Continuar de onde parei"}
            </button>
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              {uiText?.close || "Fechar"}
            </button>
            <button type="button" className="btn btn-primary" onClick={onSkipAll}>
              {uiText?.postpone || "Lembrar mais tarde"}
            </button>
          </div>
        </section>
      </div>
    </div>
  );
}

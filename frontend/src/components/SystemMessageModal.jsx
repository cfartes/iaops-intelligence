import useModalBehavior from "./useModalBehavior";

const MESSAGE_TONE_LABEL = {
  info: "Aviso",
  warning: "Alerta",
  error: "Erro",
  success: "Confirmacao",
};

export default function SystemMessageModal({ state, onClose }) {
  useModalBehavior({ open: Boolean(state?.open), onClose });
  if (!state?.open) return null;

  const tone = state.tone || "info";
  const title = state.title || MESSAGE_TONE_LABEL[tone] || "Mensagem";

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className={`modal-card tone-${tone}`}>
        <header className="modal-header">
          <h3>{title}</h3>
        </header>
        <section className="modal-content">
          <p>{state.message}</p>
        </section>
        <footer className="modal-actions">
          <button type="button" className="btn btn-primary" onClick={onClose}>
            Fechar
          </button>
        </footer>
      </div>
    </div>
  );
}

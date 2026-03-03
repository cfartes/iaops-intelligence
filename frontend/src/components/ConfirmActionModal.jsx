import { createPortal } from "react-dom";
import useModalBehavior from "./useModalBehavior";

export default function ConfirmActionModal({ open, title, message, confirmLabel, onConfirm, onClose, loading }) {
  useModalBehavior({ open, onClose });

  if (!open) return null;

  const node = (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{title}</h3>
        </header>
        <section className="modal-content">
          <p>{message}</p>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose} disabled={loading}>
              Cancelar
            </button>
            <button type="button" className="btn btn-primary" onClick={onConfirm} disabled={loading}>
              {confirmLabel}
            </button>
          </div>
        </section>
      </div>
    </div>
  );

  return createPortal(node, document.body);
}

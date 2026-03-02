import { useMemo, useState } from "react";

const FORM_TEMPLATE = {
  clientName: "",
  legalName: "",
  cnpj: "",
  contactEmail: "",
};

export default function EntityFormModal({ open, title, onClose, onSubmit }) {
  const [form, setForm] = useState(FORM_TEMPLATE);

  const canSubmit = useMemo(
    () => form.clientName && form.legalName && form.cnpj && form.contactEmail,
    [form]
  );

  if (!open) return null;

  const updateField = (field, value) => setForm((prev) => ({ ...prev, [field]: value }));

  const handleSubmit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit(form);
    setForm(FORM_TEMPLATE);
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{title}</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={handleSubmit}>
          <label>
            Nome Fantasia
            <input value={form.clientName} onChange={(e) => updateField("clientName", e.target.value)} />
          </label>
          <label>
            Razao Social
            <input value={form.legalName} onChange={(e) => updateField("legalName", e.target.value)} />
          </label>
          <label>
            CNPJ
            <input value={form.cnpj} onChange={(e) => updateField("cnpj", e.target.value)} />
          </label>
          <label>
            E-mail Contato
            <input
              type="email"
              value={form.contactEmail}
              onChange={(e) => updateField("contactEmail", e.target.value)}
            />
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit}>
              Salvar
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
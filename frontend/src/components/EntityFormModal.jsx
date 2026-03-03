import { useEffect, useMemo, useState } from "react";
import useModalBehavior from "./useModalBehavior";

const buildFormTemplate = (defaultLanguage = "pt-BR") => ({
  clientName: "",
  legalName: "",
  cnpj: "",
  contactEmail: "",
  languageCode: defaultLanguage,
});

const DEFAULT_LABELS = {
  clientName: "Nome Fantasia",
  legalName: "Razao Social",
  cnpj: "CNPJ",
  contactEmail: "E-mail Contato",
  language: "Idioma",
  cancel: "Cancelar",
  save: "Salvar",
};

const DEFAULT_LANGUAGE_OPTIONS = [
  { value: "pt-BR", label: "Portugues (Brasil)" },
  { value: "en-US", label: "English (US)" },
  { value: "es-ES", label: "Espanol" },
];

export default function EntityFormModal({
  open,
  title,
  onClose,
  onSubmit,
  defaultLanguage = "pt-BR",
  labels = DEFAULT_LABELS,
  languageOptions = DEFAULT_LANGUAGE_OPTIONS,
}) {
  useModalBehavior({ open, onClose });
  const [form, setForm] = useState(() => buildFormTemplate(defaultLanguage));

  const canSubmit = useMemo(
    () => form.clientName && form.legalName && form.cnpj && form.contactEmail,
    [form]
  );

  useEffect(() => {
    if (open) {
      setForm(buildFormTemplate(defaultLanguage));
    }
  }, [open, defaultLanguage]);

  if (!open) return null;

  const updateField = (field, value) => setForm((prev) => ({ ...prev, [field]: value }));

  const handleSubmit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit(form);
    setForm(buildFormTemplate(defaultLanguage));
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{title}</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={handleSubmit}>
          <label>
            {labels.clientName}
            <input value={form.clientName} onChange={(e) => updateField("clientName", e.target.value)} />
          </label>
          <label>
            {labels.legalName}
            <input value={form.legalName} onChange={(e) => updateField("legalName", e.target.value)} />
          </label>
          <label>
            {labels.cnpj}
            <input value={form.cnpj} onChange={(e) => updateField("cnpj", e.target.value)} />
          </label>
          <label>
            {labels.contactEmail}
            <input
              type="email"
              value={form.contactEmail}
              onChange={(e) => updateField("contactEmail", e.target.value)}
            />
          </label>
          <label>
            {labels.language}
            <select value={form.languageCode} onChange={(e) => updateField("languageCode", e.target.value)}>
              {languageOptions.map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              {labels.cancel}
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit}>
              {labels.save}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

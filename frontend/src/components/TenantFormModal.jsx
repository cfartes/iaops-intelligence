import { useMemo, useState } from "react";

const INITIAL_FORM = {
  name: "",
  slug: "",
};

export default function TenantFormModal({ open, onClose, onSubmit, loading }) {
  const [form, setForm] = useState(INITIAL_FORM);
  const canSubmit = useMemo(() => form.name.trim().length > 1 && form.slug.trim().length > 1, [form]);

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      name: form.name.trim(),
      slug: form.slug.trim().toLowerCase(),
    });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Novo Tenant</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Nome do tenant
            <input value={form.name} onChange={(e) => setForm((prev) => ({ ...prev, name: e.target.value }))} />
          </label>
          <label>
            Slug
            <input value={form.slug} onChange={(e) => setForm((prev) => ({ ...prev, slug: e.target.value }))} />
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" disabled={loading} onClick={onClose}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={loading || !canSubmit}>
              Cadastrar
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

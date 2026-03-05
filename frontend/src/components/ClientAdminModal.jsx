import { createPortal } from "react-dom";
import useModalBehavior from "./useModalBehavior";

const digitsOnly = (value) => String(value || "").replace(/\D+/g, "");

const maskCpfCnpj = (value) => {
  const v = digitsOnly(value).slice(0, 14);
  if (v.length <= 11) {
    return v
      .replace(/^(\d{3})(\d)/, "$1.$2")
      .replace(/^(\d{3})\.(\d{3})(\d)/, "$1.$2.$3")
      .replace(/\.(\d{3})(\d)/, ".$1-$2");
  }
  return v
    .replace(/^(\d{2})(\d)/, "$1.$2")
    .replace(/^(\d{2})\.(\d{3})(\d)/, "$1.$2.$3")
    .replace(/\.(\d{3})(\d)/, ".$1/$2")
    .replace(/(\d{4})(\d)/, "$1-$2");
};

const maskPhone = (value) => {
  const v = digitsOnly(value).slice(0, 11);
  if (v.length <= 10) return v.replace(/^(\d{2})(\d)/, "($1) $2").replace(/(\d{4})(\d)/, "$1-$2");
  return v.replace(/^(\d{2})(\d)/, "($1) $2").replace(/(\d{5})(\d)/, "$1-$2");
};

const maskCep = (value) => digitsOnly(value).slice(0, 8).replace(/^(\d{5})(\d)/, "$1-$2");

export default function ClientAdminModal({
  open,
  loading,
  draft,
  plans,
  onChange,
  onClose,
  onSave,
}) {
  useModalBehavior({ open, onClose });
  if (!open) return null;

  const node = (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Editar cliente</h3>
        </header>
        <section className="modal-content form-grid">
          <label>
            Nome Fantasia
            <input value={draft.trade_name || ""} onChange={(e) => onChange("trade_name", e.target.value)} />
          </label>
          <label>
            Razao Social
            <input value={draft.legal_name || ""} onChange={(e) => onChange("legal_name", e.target.value)} />
          </label>
          <label>
            CPF/CNPJ
            <input value={draft.cnpj || ""} onChange={(e) => onChange("cnpj", maskCpfCnpj(e.target.value))} />
          </label>
          <label>
            CEP
            <input value={draft.cep || ""} onChange={(e) => onChange("cep", maskCep(e.target.value))} />
          </label>
          <label>
            Endereco
            <input value={draft.address_text || ""} onChange={(e) => onChange("address_text", e.target.value)} />
          </label>
          <label>
            Bairro
            <input value={draft.bairro || ""} onChange={(e) => onChange("bairro", e.target.value)} />
          </label>
          <label>
            Cidade
            <input value={draft.cidade || ""} onChange={(e) => onChange("cidade", e.target.value)} />
          </label>
          <label>
            UF
            <input value={draft.uf || ""} onChange={(e) => onChange("uf", String(e.target.value || "").toUpperCase().slice(0, 2))} />
          </label>
          <label>
            Telefone
            <input value={draft.phone_contact || ""} onChange={(e) => onChange("phone_contact", maskPhone(e.target.value))} />
          </label>
          <label>
            E-mail login
            <input type="email" value={draft.email_access || ""} onChange={(e) => onChange("email_access", e.target.value)} />
          </label>
          <label>
            E-mail financeiro
            <input type="email" value={draft.email_financial || ""} onChange={(e) => onChange("email_financial", e.target.value)} />
          </label>
          <label>
            E-mail NFs
            <input type="email" value={draft.email_nf || ""} onChange={(e) => onChange("email_nf", e.target.value)} />
          </label>
          <label>
            Plano
            <select value={draft.plan_code || ""} onChange={(e) => onChange("plan_code", e.target.value)}>
              <option value="">Sem alteracao</option>
              {(plans || []).map((item) => (
                <option key={String(item.id || item.code)} value={item.code}>
                  {item.name} ({item.code})
                </option>
              ))}
            </select>
          </label>
          <label>
            Status
            <select value={draft.status || "active"} onChange={(e) => onChange("status", e.target.value)}>
              <option value="active">Ativo</option>
              <option value="inactive">Inativo</option>
              <option value="blocked">Bloqueado</option>
            </select>
          </label>
        </section>
        <div className="modal-actions">
          <button type="button" className="btn btn-secondary" onClick={onClose} disabled={loading}>
            Cancelar
          </button>
          <button type="button" className="btn btn-primary" onClick={onSave} disabled={loading}>
            Salvar
          </button>
        </div>
      </div>
    </div>
  );

  return createPortal(node, document.body);
}


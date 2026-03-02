import { useMemo, useState } from "react";

const FORM_TEMPLATE = {
  trade_name: "",
  legal_name: "",
  cnpj: "",
  address_text: "",
  phone_contact: "",
  email_contact: "",
  email_access: "",
  email_notification: "",
  password: "",
  plan_code: "starter",
  language_code: "pt-BR",
};

const PLAN_OPTIONS = [
  { value: "starter", label: "Starter" },
  { value: "pro", label: "Pro" },
  { value: "enterprise", label: "Enterprise" },
];

export default function ClientSignupModal({ open, onClose, onSignup, onConfirm, labels }) {
  const [form, setForm] = useState(FORM_TEMPLATE);
  const [phase, setPhase] = useState("signup");
  const [token, setToken] = useState("");
  const [busy, setBusy] = useState(false);
  const [signupHint, setSignupHint] = useState("");

  const canSignup = useMemo(() => {
    return (
      form.trade_name &&
      form.legal_name &&
      form.cnpj &&
      form.address_text &&
      form.phone_contact &&
      form.email_contact &&
      form.email_access &&
      form.email_notification &&
      form.password &&
      form.plan_code &&
      form.language_code
    );
  }, [form]);

  if (!open) return null;

  const updateField = (field, value) => setForm((prev) => ({ ...prev, [field]: value }));

  const handleSignup = async (event) => {
    event.preventDefault();
    if (!canSignup || busy) return;
    setBusy(true);
    try {
      const data = await onSignup(form);
      setPhase("confirm");
      setSignupHint(data?.delivery || "");
      if (data?.confirm_token) {
        setToken(data.confirm_token);
      }
    } finally {
      setBusy(false);
    }
  };

  const handleConfirm = async (event) => {
    event.preventDefault();
    if (!token.trim() || busy) return;
    setBusy(true);
    try {
      await onConfirm({ confirm_token: token.trim() });
      setForm(FORM_TEMPLATE);
      setToken("");
      setPhase("signup");
      setSignupHint("");
      onClose();
    } finally {
      setBusy(false);
    }
  };

  const resetAndClose = () => {
    if (busy) return;
    setForm(FORM_TEMPLATE);
    setToken("");
    setPhase("signup");
    setSignupHint("");
    onClose();
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{labels.title}</h3>
        </header>
        {phase === "signup" ? (
          <form className="modal-content form-grid" onSubmit={handleSignup}>
            <label>
              {labels.trade_name}
              <input value={form.trade_name} onChange={(e) => updateField("trade_name", e.target.value)} />
            </label>
            <label>
              {labels.legal_name}
              <input value={form.legal_name} onChange={(e) => updateField("legal_name", e.target.value)} />
            </label>
            <label>
              {labels.cnpj}
              <input value={form.cnpj} onChange={(e) => updateField("cnpj", e.target.value)} />
            </label>
            <label>
              {labels.address_text}
              <input value={form.address_text} onChange={(e) => updateField("address_text", e.target.value)} />
            </label>
            <label>
              {labels.phone_contact}
              <input value={form.phone_contact} onChange={(e) => updateField("phone_contact", e.target.value)} />
            </label>
            <label>
              {labels.email_contact}
              <input type="email" value={form.email_contact} onChange={(e) => updateField("email_contact", e.target.value)} />
            </label>
            <label>
              {labels.email_access}
              <input type="email" value={form.email_access} onChange={(e) => updateField("email_access", e.target.value)} />
            </label>
            <label>
              {labels.email_notification}
              <input
                type="email"
                value={form.email_notification}
                onChange={(e) => updateField("email_notification", e.target.value)}
              />
            </label>
            <label>
              {labels.password}
              <input type="password" value={form.password} onChange={(e) => updateField("password", e.target.value)} />
            </label>
            <label>
              {labels.plan_code}
              <select value={form.plan_code} onChange={(e) => updateField("plan_code", e.target.value)}>
                {PLAN_OPTIONS.map((item) => (
                  <option key={item.value} value={item.value}>
                    {item.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              {labels.language_code}
              <select value={form.language_code} onChange={(e) => updateField("language_code", e.target.value)}>
                <option value="pt-BR">Portugues (Brasil)</option>
                <option value="en-US">English (US)</option>
                <option value="es-ES">Espanol</option>
              </select>
            </label>
            <div className="modal-actions">
              <button type="button" className="btn btn-secondary" onClick={resetAndClose}>
                {labels.cancel}
              </button>
              <button type="submit" className="btn btn-primary" disabled={!canSignup || busy}>
                {busy ? labels.submitting : labels.submit}
              </button>
            </div>
          </form>
        ) : (
          <form className="modal-content form-grid" onSubmit={handleConfirm}>
            <p className="muted">{labels.confirm_intro}</p>
            {signupHint && <p className="muted">{signupHint}</p>}
            <label>
              {labels.confirm_token}
              <input value={token} onChange={(e) => setToken(e.target.value)} />
            </label>
            <div className="modal-actions">
              <button type="button" className="btn btn-secondary" onClick={resetAndClose}>
                {labels.cancel}
              </button>
              <button type="submit" className="btn btn-primary" disabled={!token.trim() || busy}>
                {busy ? labels.confirming : labels.confirm}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}


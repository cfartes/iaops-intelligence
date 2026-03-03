import { useEffect, useMemo, useState } from "react";

const INITIAL_FORM = {
  host: "",
  port: 587,
  user: "",
  from_email: "",
  starttls: true,
  password: "",
  clear_password: false,
};

export default function SmtpConfigModal({ open, initialConfig, loading, onClose, onSubmit, onTest }) {
  const [form, setForm] = useState(INITIAL_FORM);
  const [testEmail, setTestEmail] = useState("");

  useEffect(() => {
    if (!open) return;
    setForm({
      host: initialConfig?.host || "",
      port: Number(initialConfig?.port || 587),
      user: initialConfig?.user || "",
      from_email: initialConfig?.from_email || "",
      starttls: Boolean(initialConfig?.starttls),
      password: "",
      clear_password: false,
    });
    setTestEmail(initialConfig?.from_email || "");
  }, [open, initialConfig]);

  const canSubmit = useMemo(() => form.host.trim() && Number(form.port) > 0, [form]);

  if (!open) return null;

  const buildPayload = () => ({
      host: form.host.trim(),
      port: Number(form.port),
      user: form.user.trim(),
      from_email: form.from_email.trim(),
      starttls: Boolean(form.starttls),
      ...(form.password.trim() ? { password: form.password } : {}),
      ...(form.clear_password ? { clear_password: true } : {}),
    });

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit(buildPayload());
  };

  const testConnection = () => {
    if (!canSubmit || !onTest) return;
    onTest(buildPayload());
  };

  const sendTestEmail = () => {
    if (!canSubmit || !onTest || !testEmail.trim()) return;
    onTest({ ...buildPayload(), to_email: testEmail.trim() }, { sendMail: true });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>Configurar SMTP (Superadmin)</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Host SMTP
            <input value={form.host} onChange={(e) => setForm((prev) => ({ ...prev, host: e.target.value }))} />
          </label>
          <label>
            Porta
            <input
              type="number"
              min={1}
              max={65535}
              value={form.port}
              onChange={(e) => setForm((prev) => ({ ...prev, port: e.target.value }))}
            />
          </label>
          <label>
            Usuario SMTP
            <input value={form.user} onChange={(e) => setForm((prev) => ({ ...prev, user: e.target.value }))} />
          </label>
          <label>
            E-mail remetente
            <input value={form.from_email} onChange={(e) => setForm((prev) => ({ ...prev, from_email: e.target.value }))} />
          </label>
          <label>
            Nova senha (opcional)
            <input
              type="password"
              value={form.password}
              onChange={(e) => setForm((prev) => ({ ...prev, password: e.target.value }))}
              placeholder="Preencha para atualizar"
            />
          </label>
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={form.starttls}
              onChange={(e) => setForm((prev) => ({ ...prev, starttls: e.target.checked }))}
            />
            Usar STARTTLS
          </label>
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={form.clear_password}
              onChange={(e) => setForm((prev) => ({ ...prev, clear_password: e.target.checked }))}
            />
            Limpar senha salva
          </label>
          <label>
            E-mail destino para teste
            <input
              type="email"
              value={testEmail}
              onChange={(e) => setTestEmail(e.target.value)}
              placeholder="destino@dominio.com"
            />
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" disabled={loading} onClick={onClose}>
              Cancelar
            </button>
            <button type="button" className="btn btn-secondary" disabled={!canSubmit || loading} onClick={testConnection}>
              Testar conexao
            </button>
            <button type="button" className="btn btn-secondary" disabled={!canSubmit || !testEmail.trim() || loading} onClick={sendTestEmail}>
              Enviar e-mail teste
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit || loading}>
              Salvar SMTP
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

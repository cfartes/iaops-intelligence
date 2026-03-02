import { useEffect, useMemo, useState } from "react";

export default function MfaCodeModal({
  open,
  title,
  subtitle,
  setupInfo,
  submitLabel,
  loading,
  onClose,
  onSubmit,
}) {
  const [otpCode, setOtpCode] = useState("");
  const canSubmit = useMemo(() => otpCode.trim().length >= 6, [otpCode]);

  useEffect(() => {
    if (open) setOtpCode("");
  }, [open]);

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({ otp_code: otpCode.trim() });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{title}</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          {subtitle ? <p>{subtitle}</p> : null}
          {setupInfo ? (
            <div className="table-wrap">
              <table className="data-table">
                <tbody>
                  <tr>
                    <th>Secret</th>
                    <td>{setupInfo.secret}</td>
                  </tr>
                  <tr>
                    <th>URI</th>
                    <td>{setupInfo.provisioning_uri}</td>
                  </tr>
                  <tr>
                    <th>Expira em</th>
                    <td>{setupInfo.expires_at}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          ) : null}
          <label>
            Codigo TOTP (6 digitos)
            <input value={otpCode} onChange={(e) => setOtpCode(e.target.value)} />
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose} disabled={loading}>
              Cancelar
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit || loading}>
              {submitLabel}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

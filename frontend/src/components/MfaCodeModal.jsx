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
  const [showQr, setShowQr] = useState(true);
  const canSubmit = useMemo(() => otpCode.trim().length >= 6, [otpCode]);
  const qrCodeUrl = useMemo(() => {
    const uri = String(setupInfo?.provisioning_uri || "").trim();
    if (!uri) return "";
    return `https://api.qrserver.com/v1/create-qr-code/?size=220x220&data=${encodeURIComponent(uri)}`;
  }, [setupInfo?.provisioning_uri]);

  useEffect(() => {
    if (open) {
      setOtpCode("");
      setShowQr(true);
    }
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
            <div>
              {qrCodeUrl && showQr ? (
                <div className="table-wrap" style={{ marginBottom: "0.75rem" }}>
                  <img
                    src={qrCodeUrl}
                    alt="QR Code MFA TOTP"
                    width={220}
                    height={220}
                    onError={() => setShowQr(false)}
                  />
                </div>
              ) : null}
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

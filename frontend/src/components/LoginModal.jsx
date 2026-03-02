import { useMemo, useState } from "react";

export default function LoginModal({ open, labels, onLogin, onVerifyMfa }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [otpCode, setOtpCode] = useState("");
  const [challengeToken, setChallengeToken] = useState("");
  const [busy, setBusy] = useState(false);

  const canLogin = useMemo(() => email.trim() && password, [email, password]);
  const canVerify = useMemo(() => challengeToken && otpCode.trim(), [challengeToken, otpCode]);

  if (!open) return null;

  const handleLogin = async (event) => {
    event.preventDefault();
    if (!canLogin || busy) return;
    setBusy(true);
    try {
      const data = await onLogin({ email: email.trim(), password });
      if (data?.mfa_required && data?.challenge_token) {
        setChallengeToken(data.challenge_token);
      }
    } finally {
      setBusy(false);
    }
  };

  const handleVerify = async (event) => {
    event.preventDefault();
    if (!canVerify || busy) return;
    setBusy(true);
    try {
      await onVerifyMfa({ challenge_token: challengeToken, otp_code: otpCode.trim() });
      setEmail("");
      setPassword("");
      setOtpCode("");
      setChallengeToken("");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{labels.title}</h3>
        </header>
        {!challengeToken ? (
          <form className="modal-content form-grid" onSubmit={handleLogin}>
            <label>
              {labels.email}
              <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} />
            </label>
            <label>
              {labels.password}
              <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
            </label>
            <div className="modal-actions">
              <button type="submit" className="btn btn-primary" disabled={!canLogin || busy}>
                {busy ? labels.logging : labels.login}
              </button>
            </div>
          </form>
        ) : (
          <form className="modal-content form-grid" onSubmit={handleVerify}>
            <p className="muted">{labels.mfa_intro}</p>
            <label>
              {labels.otp_code}
              <input value={otpCode} onChange={(e) => setOtpCode(e.target.value)} />
            </label>
            <div className="modal-actions">
              <button type="submit" className="btn btn-primary" disabled={!canVerify || busy}>
                {busy ? labels.verifying : labels.verify}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

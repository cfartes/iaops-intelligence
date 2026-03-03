import { useMemo, useState } from "react";

const SIGNUP_TEMPLATE = {
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

export default function AuthScreen({
  labels,
  onLogin,
  onVerifyMfa,
  onSignup,
  onConfirm,
  onPasswordResetRequest,
  onPasswordResetConfirm,
}) {
  const [tab, setTab] = useState("signin");
  const [signinMode, setSigninMode] = useState("login");
  const [busy, setBusy] = useState(false);
  const [challengeToken, setChallengeToken] = useState("");
  const [otpCode, setOtpCode] = useState("");
  const [loginEmail, setLoginEmail] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [resetEmail, setResetEmail] = useState("");
  const [resetToken, setResetToken] = useState("");
  const [resetPassword, setResetPassword] = useState("");
  const [resetHint, setResetHint] = useState("");
  const [signupPhase, setSignupPhase] = useState("signup");
  const [signupForm, setSignupForm] = useState(SIGNUP_TEMPLATE);
  const [confirmToken, setConfirmToken] = useState("");
  const [signupHint, setSignupHint] = useState("");

  const canLogin = useMemo(() => loginEmail.trim() && loginPassword, [loginEmail, loginPassword]);
  const canVerify = useMemo(() => challengeToken && otpCode.trim().length >= 6, [challengeToken, otpCode]);
  const canResetRequest = useMemo(() => resetEmail.trim().length > 5, [resetEmail]);
  const canResetConfirm = useMemo(() => resetToken.trim() && resetPassword.trim().length >= 8, [resetToken, resetPassword]);
  const canSignup = useMemo(
    () =>
      signupForm.trade_name &&
      signupForm.legal_name &&
      signupForm.cnpj &&
      signupForm.address_text &&
      signupForm.phone_contact &&
      signupForm.email_contact &&
      signupForm.email_access &&
      signupForm.email_notification &&
      signupForm.password &&
      signupForm.plan_code &&
      signupForm.language_code,
    [signupForm]
  );
  const authLabels = labels?.auth || {};
  const authTitle = authLabels.title || "IAOps Governance";
  const authHeadline = authLabels.headline || "Governanca inteligente para operacao de dados";
  const authLead =
    authLabels.lead ||
    "Inventario de metadados, deteccao de mudancas, LGPD e consultas naturais em um unico painel.";
  const authBenefits = Array.isArray(authLabels.benefits) ? authLabels.benefits : [];
  const languageOptions = Array.isArray(authLabels.language_options) ? authLabels.language_options : [];

  const resetSignup = () => {
    setSignupForm(SIGNUP_TEMPLATE);
    setSignupPhase("signup");
    setConfirmToken("");
    setSignupHint("");
  };

  const submitLogin = async (event) => {
    event.preventDefault();
    if (!canLogin || busy) return;
    setBusy(true);
    try {
      const data = await onLogin({ email: loginEmail.trim(), password: loginPassword });
      if (data?.mfa_required && data?.challenge_token) {
        setChallengeToken(data.challenge_token);
      } else {
        setChallengeToken("");
        setOtpCode("");
      }
    } finally {
      setBusy(false);
    }
  };

  const submitVerify = async (event) => {
    event.preventDefault();
    if (!canVerify || busy) return;
    setBusy(true);
    try {
      await onVerifyMfa({ challenge_token: challengeToken, otp_code: otpCode.trim() });
      setChallengeToken("");
      setOtpCode("");
    } finally {
      setBusy(false);
    }
  };

  const submitPasswordResetRequest = async (event) => {
    event.preventDefault();
    if (!canResetRequest || busy || !onPasswordResetRequest) return;
    setBusy(true);
    try {
      const data = await onPasswordResetRequest({ email_access: resetEmail.trim() });
      setResetHint(data?.delivery || "");
      if (data?.signup_pending) {
        setSignupPhase("confirm");
        setTab("signup");
        if (data?.confirm_token) {
          setConfirmToken(data.confirm_token);
        }
        return;
      }
      if (data?.reset_token) {
        setResetToken(data.reset_token);
      }
      setSigninMode("forgot_confirm");
    } finally {
      setBusy(false);
    }
  };

  const submitPasswordResetConfirm = async (event) => {
    event.preventDefault();
    if (!canResetConfirm || busy || !onPasswordResetConfirm) return;
    setBusy(true);
    try {
      await onPasswordResetConfirm({ reset_token: resetToken.trim(), new_password: resetPassword });
      setSigninMode("login");
      setChallengeToken("");
      setOtpCode("");
      setLoginPassword("");
      setResetToken("");
      setResetPassword("");
      setResetHint("");
    } finally {
      setBusy(false);
    }
  };

  const submitSignup = async (event) => {
    event.preventDefault();
    if (!canSignup || busy) return;
    setBusy(true);
    try {
      const data = await onSignup(signupForm);
      setSignupPhase("confirm");
      setSignupHint(data?.delivery || "");
      if (data?.confirm_token) {
        setConfirmToken(data.confirm_token);
      }
    } finally {
      setBusy(false);
    }
  };

  const submitConfirm = async (event) => {
    event.preventDefault();
    if (!confirmToken.trim() || busy) return;
    setBusy(true);
    try {
      await onConfirm({ confirm_token: confirmToken.trim() });
      resetSignup();
      setTab("signin");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="auth-screen">
      <div className="auth-card">
        <aside className="auth-hero">
          <p className="auth-kicker">{authTitle}</p>
          <h1>{authHeadline}</h1>
          <p className="auth-lead">{authLead}</p>
          <ul className="auth-benefits">
            {(authBenefits.length > 0
              ? authBenefits
              : [
                  "Multi-tenant com trilha de auditoria completa",
                  "Chat BI, WhatsApp e Telegram com linguagem natural",
                  "Controles de acesso, MFA e bloqueio por risco",
                ]
            ).map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </aside>

        <section className="auth-form-wrap">
          <div className="auth-tabs">
            <button
              type="button"
              className={`auth-tab ${tab === "signin" ? "active" : ""}`}
              onClick={() => setTab("signin")}
            >
              {labels.signIn}
            </button>
            <button
              type="button"
              className={`auth-tab ${tab === "signup" ? "active" : ""}`}
              onClick={() => setTab("signup")}
            >
              {labels.signUp}
            </button>
          </div>

          {tab === "signin" ? (
            <form className="modal-content form-grid auth-form" onSubmit={challengeToken ? submitVerify : submitLogin}>
              <h3>{labels.login.title}</h3>
              {signinMode === "forgot_request" ? (
                <>
                  <p className="muted">{labels.login.reset_intro}</p>
                  <label>
                    {labels.login.email}
                    <input type="email" value={resetEmail} onChange={(e) => setResetEmail(e.target.value)} />
                  </label>
                  <div className="modal-actions auth-actions">
                    <button type="button" className="btn btn-secondary" onClick={() => setSigninMode("login")} disabled={busy}>
                      {labels.login.back}
                    </button>
                    <button type="button" className="btn btn-primary" onClick={submitPasswordResetRequest} disabled={!canResetRequest || busy}>
                      {busy ? labels.login.reset_requesting : labels.login.reset_request}
                    </button>
                  </div>
                </>
              ) : signinMode === "forgot_confirm" ? (
                <>
                  <p className="muted">{labels.login.reset_confirm_intro}</p>
                  {resetHint ? <p className="muted">{resetHint}</p> : null}
                  <label>
                    {labels.login.reset_token}
                    <input value={resetToken} onChange={(e) => setResetToken(e.target.value)} />
                  </label>
                  <label>
                    {labels.login.reset_new_password}
                    <input type="password" value={resetPassword} onChange={(e) => setResetPassword(e.target.value)} />
                  </label>
                  <div className="modal-actions auth-actions">
                    <button type="button" className="btn btn-secondary" onClick={() => setSigninMode("login")} disabled={busy}>
                      {labels.login.back}
                    </button>
                    <button type="button" className="btn btn-primary" onClick={submitPasswordResetConfirm} disabled={!canResetConfirm || busy}>
                      {busy ? labels.login.reset_confirming : labels.login.reset_confirm}
                    </button>
                  </div>
                </>
              ) : !challengeToken ? (
                <>
                  <label>
                    {labels.login.email}
                    <input type="email" value={loginEmail} onChange={(e) => setLoginEmail(e.target.value)} />
                  </label>
                  <label>
                    {labels.login.password}
                    <input type="password" value={loginPassword} onChange={(e) => setLoginPassword(e.target.value)} />
                  </label>
                  <div className="modal-actions auth-actions">
                    <button
                      type="button"
                      className="btn btn-secondary"
                      onClick={() => {
                        setResetEmail(loginEmail || "");
                        setSigninMode("forgot_request");
                      }}
                      disabled={busy}
                    >
                      {labels.login.forgot_password}
                    </button>
                    <button type="submit" className="btn btn-primary" disabled={!canLogin || busy}>
                      {busy ? labels.login.logging : labels.login.login}
                    </button>
                  </div>
                </>
              ) : (
                <>
                  <p className="muted">{labels.login.mfa_intro}</p>
                  <label>
                    {labels.login.otp_code}
                    <input value={otpCode} onChange={(e) => setOtpCode(e.target.value)} />
                  </label>
                  <div className="modal-actions auth-actions">
                    <button type="submit" className="btn btn-primary" disabled={!canVerify || busy}>
                      {busy ? labels.login.verifying : labels.login.verify}
                    </button>
                  </div>
                </>
              )}
            </form>
          ) : (
            <form className="modal-content form-grid auth-form" onSubmit={signupPhase === "signup" ? submitSignup : submitConfirm}>
              <h3>{labels.signup.title}</h3>
              {signupPhase === "signup" ? (
                <div className="auth-signup-grid">
                  <label>
                    {labels.signup.trade_name}
                    <input value={signupForm.trade_name} onChange={(e) => setSignupForm((prev) => ({ ...prev, trade_name: e.target.value }))} />
                  </label>
                  <label>
                    {labels.signup.legal_name}
                    <input value={signupForm.legal_name} onChange={(e) => setSignupForm((prev) => ({ ...prev, legal_name: e.target.value }))} />
                  </label>
                  <label>
                    {labels.signup.cnpj}
                    <input value={signupForm.cnpj} onChange={(e) => setSignupForm((prev) => ({ ...prev, cnpj: e.target.value }))} />
                  </label>
                  <label>
                    {labels.signup.address_text}
                    <input value={signupForm.address_text} onChange={(e) => setSignupForm((prev) => ({ ...prev, address_text: e.target.value }))} />
                  </label>
                  <label>
                    {labels.signup.phone_contact}
                    <input value={signupForm.phone_contact} onChange={(e) => setSignupForm((prev) => ({ ...prev, phone_contact: e.target.value }))} />
                  </label>
                  <label>
                    {labels.signup.email_contact}
                    <input type="email" value={signupForm.email_contact} onChange={(e) => setSignupForm((prev) => ({ ...prev, email_contact: e.target.value }))} />
                  </label>
                  <label>
                    {labels.signup.email_access}
                    <input type="email" value={signupForm.email_access} onChange={(e) => setSignupForm((prev) => ({ ...prev, email_access: e.target.value }))} />
                  </label>
                  <label>
                    {labels.signup.email_notification}
                    <input type="email" value={signupForm.email_notification} onChange={(e) => setSignupForm((prev) => ({ ...prev, email_notification: e.target.value }))} />
                  </label>
                  <label>
                    {labels.signup.password}
                    <input type="password" value={signupForm.password} onChange={(e) => setSignupForm((prev) => ({ ...prev, password: e.target.value }))} />
                  </label>
                  <label>
                    {labels.signup.plan_code}
                    <select value={signupForm.plan_code} onChange={(e) => setSignupForm((prev) => ({ ...prev, plan_code: e.target.value }))}>
                      {PLAN_OPTIONS.map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    {labels.signup.language_code}
                    <select value={signupForm.language_code} onChange={(e) => setSignupForm((prev) => ({ ...prev, language_code: e.target.value }))}>
                      {(languageOptions.length > 0
                        ? languageOptions
                        : [
                            { value: "pt-BR", label: "Portugues (Brasil)" },
                            { value: "en-US", label: "English (US)" },
                            { value: "es-ES", label: "Espanol" },
                          ]
                      ).map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <div className="modal-actions auth-actions">
                    <button type="submit" className="btn btn-primary" disabled={!canSignup || busy}>
                      {busy ? labels.signup.submitting : labels.signup.submit}
                    </button>
                  </div>
                </div>
              ) : (
                <>
                  <p className="muted">{labels.signup.confirm_intro}</p>
                  {signupHint ? <p className="muted">{signupHint}</p> : null}
                  <label>
                    {labels.signup.confirm_token}
                    <input value={confirmToken} onChange={(e) => setConfirmToken(e.target.value)} />
                  </label>
                  <div className="modal-actions auth-actions">
                    <button type="submit" className="btn btn-primary" disabled={!confirmToken.trim() || busy}>
                      {busy ? labels.signup.confirming : labels.signup.confirm}
                    </button>
                  </div>
                </>
              )}
            </form>
          )}
        </section>
      </div>
    </div>
  );
}

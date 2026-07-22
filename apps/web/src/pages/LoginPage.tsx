import { FormEvent, useEffect, useMemo, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { DtLogo } from "../components/DtLogo";
import { useToast } from "../components/Toast";
import { fetchSsoProviders, loginWorkspace, ssoStartUrl, SsoType } from "../lib/api";
import { writeSession } from "../lib/session";
import { Screen } from "../lib/types";

interface LoginPageProps {
  target: Screen;
  onAuthenticated: (email: string) => void;
  onBack: () => void;
}

function isValidEmail(value: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());
}

const TARGET_LABELS: Partial<Record<Screen, string>> = {
  dashboard: "Overview",
  transfer: "Transfer Studio",
  pilot: "Data Pilot",
  connectors: "Connectors",
  jobs: "Job Theater",
  settings: "Settings",
};

const TRUST_METRICS = [
  { value: "8", label: "Preflight gates" },
  { value: "Map", label: "Semantic confidence" },
  { value: "Σ", label: "Checksum proof" },
];

export function LoginPage({ target, onAuthenticated, onBack }: LoginPageProps) {
  const { toast } = useToast();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [remember, setRemember] = useState(true);
  const [submitted, setSubmitted] = useState(false);
  const [emailTouched, setEmailTouched] = useState(false);
  const [passwordTouched, setPasswordTouched] = useState(false);
  const [checking, setChecking] = useState(false);
  const [credentialError, setCredentialError] = useState("");
  const [capsLock, setCapsLock] = useState(false);
  const [ssoProviders, setSsoProviders] = useState<Array<{ type: SsoType; label: string; login_path: string }>>([]);

  useEffect(() => {
    fetchSsoProviders().then(setSsoProviders).catch(() => setSsoProviders([]));
  }, []);

  const emailTrimmed = email.trim();
  const showEmailError = (submitted || emailTouched) && email.length > 0 && !isValidEmail(email);
  const showEmailRequired = submitted && emailTrimmed.length === 0;
  const emailError = showEmailRequired
    ? "Work email is required."
    : showEmailError
      ? "Enter a valid work email (name@company.com)."
      : "";

  const passwordTooShort = password.length > 0 && password.length < 8;
  const showPasswordRequired = submitted && password.length === 0;
  const showPasswordShort = (submitted || passwordTouched) && passwordTooShort;
  const passwordError = showPasswordRequired
    ? "Password is required."
    : showPasswordShort
      ? "Use at least 8 characters."
      : "";

  const targetLabel = TARGET_LABELS[target] ?? "DataFlow";
  const emailOk = isValidEmail(email);
  const passwordOk = password.length >= 8;
  const ready = useMemo(() => emailOk && passwordOk, [emailOk, passwordOk]);

  const passwordChecks = [
    { ok: password.length >= 8, label: "At least 8 characters" },
    { ok: /[A-Za-z]/.test(password) && /\d/.test(password), label: "Letters and a number recommended" },
  ];

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setSubmitted(true);
    setEmailTouched(true);
    setPasswordTouched(true);
    setCredentialError("");
    if (!ready) {
      toast({
        title: "Complete the form",
        message: "Use a valid work email and a password with at least 8 characters.",
        tone: "warning",
      });
      return;
    }

    setChecking(true);
    try {
      const result = await loginWorkspace(emailTrimmed, password);
      writeSession(
        {
          email: result.user.email,
          name: result.user.name,
          role: result.user.role,
          token: result.token,
          expires_at: result.expires_at,
          signed_in_at: Date.now(),
        },
        remember,
      );
      toast({ title: "Signed in", message: `Opening ${targetLabel}.`, tone: "success" });
      onAuthenticated(result.user.email);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "";
      const apiOffline =
        msg.includes("Failed to fetch") ||
        msg.includes("NetworkError") ||
        msg.includes("timed out") ||
        (msg.includes("fetch") && !msg.includes("Sign-in"));
      if (apiOffline) {
        setCredentialError("Cannot reach the API. Confirm the control plane URL and that the API service is online.");
        toast({
          title: "API offline",
          message: "The control plane is not reachable from this browser.",
          tone: "error",
        });
      } else {
        setCredentialError(msg || "Email or password is incorrect.");
        toast({
          title: "Sign-in failed",
          message: msg || "Check your credentials and try again.",
          tone: "error",
        });
      }
    } finally {
      setChecking(false);
    }
  };

  const startSso = (type: SsoType, label: string) => {
    window.location.href = ssoStartUrl(type);
    toast({ title: `Redirecting to ${label}`, message: "Complete sign-in with your identity provider.", tone: "info" });
  };

  const enterDevPreview = () => {
    writeSession(
      {
        email: "test@gmail.com",
        name: "Test User",
        role: "admin",
        token: "dev-ui-preview",
        expires_at: Math.floor(Date.now() / 1000) + 86400,
        signed_in_at: Date.now(),
      },
      remember,
    );
    toast({
      title: "Dev preview workspace",
      message: "UI-only mode — start the API on port 8001 for live data.",
      tone: "info",
    });
    onAuthenticated("test@gmail.com");
  };

  const alertKind = credentialError.toLowerCase().includes("api")
    ? "api"
    : credentialError.toLowerCase().includes("configured")
      ? "config"
      : "auth";

  return (
    <main className="lp-login lp-login--gate">
      <section className="lp-login-brand" aria-label="DataFlow">
        <div className="lp-login-brand-bg" aria-hidden>
          <span className="lp-login-brand-mesh" />
          <span className="lp-login-brand-orb lp-login-brand-orb--a" />
          <span className="lp-login-brand-orb lp-login-brand-orb--b" />
          <span className="lp-login-brand-rail" />
        </div>

        <div className="lp-login-brand-inner">
          <header className="lp-login-brand-head">
            <button type="button" className="lp-login-back" onClick={onBack}>
              <DtIcon name="chevron-left" size={15} />
              <span>Back</span>
            </button>
            <div className="lp-login-brand-logo">
              <DtLogo size={32} />
              <span>DataFlow</span>
            </div>
          </header>

          <div className="lp-login-brand-body">
            <p className="lp-login-brand-kicker">Enterprise data platform</p>
            <h1 className="lp-login-brand-title">DataFlow</h1>
            <p className="lp-login-brand-lead">
              Governed movement for banks, warehouses, and ops teams — semantic mapping, preflight, and checksum proof on every run.
            </p>

            <div className="lp-login-metrics" role="list">
              {TRUST_METRICS.map((m) => (
                <div key={m.label} className="lp-login-metric" role="listitem">
                  <strong>{m.value}</strong>
                  <span>{m.label}</span>
                </div>
              ))}
            </div>

            <p className="lp-login-brand-dest">
              Continue to <strong>{targetLabel}</strong>
            </p>
          </div>

          <footer className="lp-login-brand-foot">
            <span>Self-host ready</span>
            <span aria-hidden>·</span>
            <span>SSO / RBAC</span>
            <span aria-hidden>·</span>
            <span>Audit trails</span>
          </footer>
        </div>
      </section>

      <section className="lp-login-auth" aria-labelledby="login-form-title">
        <div className="lp-login-auth-inner">
          <div className="lp-login-auth-head">
            <p className="lp-login-auth-kicker">Secure workspace access</p>
            <h2 id="login-form-title">Sign in</h2>
            <p className="lp-login-auth-sub">
              Authenticate with your workspace credentials. Sessions are issued by the DataFlow API — never stored as plain text.
            </p>
          </div>

          {credentialError && (
            <div
              className={`lp-login-alert ${alertKind === "auth" ? "lp-login-alert--warn" : "lp-login-alert--danger"}`}
              role="alert"
            >
              <DtIcon name="alert" size={18} />
              <div>
                <strong>
                  {alertKind === "api"
                    ? "Control plane unreachable"
                    : alertKind === "config"
                      ? "Workspace not ready"
                      : "Sign-in failed"}
                </strong>
                <p>{credentialError}</p>
              </div>
            </div>
          )}

          <form className="lp-login-form" onSubmit={submit} noValidate>
            <div className={`lp-field ${emailError ? "is-error" : emailOk && emailTrimmed ? "is-ok" : ""}`}>
              <div className="lp-label-row">
                <label className="lp-label" htmlFor="login-email">Work email</label>
                {emailOk && emailTrimmed && !emailError && (
                  <span className="lp-field-status lp-field-status--ok">Valid</span>
                )}
              </div>
              <input
                id="login-email"
                className="lp-input"
                type="email"
                inputMode="email"
                value={email}
                onChange={(e) => {
                  setEmail(e.target.value);
                  setCredentialError("");
                }}
                onBlur={() => setEmailTouched(true)}
                autoComplete="username"
                autoCapitalize="none"
                autoCorrect="off"
                spellCheck={false}
                placeholder="admin@company.com"
                aria-invalid={Boolean(emailError)}
                aria-describedby={emailError ? "login-email-error" : undefined}
              />
              {emailError && <small id="login-email-error" className="lp-field-error">{emailError}</small>}
            </div>

            <div className={`lp-field ${passwordError ? "is-error" : passwordOk ? "is-ok" : ""}`}>
              <div className="lp-label-row">
                <label className="lp-label" htmlFor="login-password">Password</label>
                {capsLock && <span className="lp-login-caps">Caps Lock on</span>}
              </div>
              <div className="lp-input-password">
                <input
                  id="login-password"
                  className="lp-input"
                  type={showPassword ? "text" : "password"}
                  value={password}
                  onChange={(e) => {
                    setPassword(e.target.value);
                    setCredentialError("");
                  }}
                  onBlur={() => setPasswordTouched(true)}
                  onKeyUp={(e) => setCapsLock(e.getModifierState?.("CapsLock") ?? false)}
                  onKeyDown={(e) => setCapsLock(e.getModifierState?.("CapsLock") ?? false)}
                  autoComplete="current-password"
                  placeholder="Enter your password"
                  aria-invalid={Boolean(passwordError || (credentialError && alertKind === "auth"))}
                  aria-describedby="login-password-hints"
                />
                <button
                  type="button"
                  className="lp-input-password-toggle"
                  onClick={() => setShowPassword((v) => !v)}
                  aria-label={showPassword ? "Hide password" : "Show password"}
                >
                  <DtIcon name={showPassword ? "lock" : "scan"} size={15} />
                </button>
              </div>
              {passwordError && <small className="lp-field-error">{passwordError}</small>}
              <ul id="login-password-hints" className="lp-login-checks" aria-live="polite">
                {passwordChecks.map((c) => (
                  <li key={c.label} className={c.ok ? "is-ok" : ""}>
                    <span className="lp-login-check-mark" aria-hidden>{c.ok ? "✓" : "○"}</span>
                    {c.label}
                  </li>
                ))}
              </ul>
            </div>

            <div className="lp-login-row">
              <label className="lp-login-remember">
                <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
                <span>Keep me signed in on this device</span>
              </label>
            </div>

            <button
              type="submit"
              className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block lp-login-submit"
              disabled={checking}
            >
              {checking ? "Verifying credentials…" : "Sign in to workspace"}
            </button>
          </form>

          {ssoProviders.length > 0 && (
            <>
              <div className="lp-login-divider"><span>Or continue with SSO</span></div>
              <div className="lp-login-sso">
                {ssoProviders.map((provider) => (
                  <button
                    key={provider.type}
                    type="button"
                    className="lp-btn lp-btn--outline lp-login-sso-btn"
                    onClick={() => startSso(provider.type, provider.label)}
                  >
                    {provider.label}
                  </button>
                ))}
              </div>
            </>
          )}

          {import.meta.env.DEV && (
            <div className="lp-login-dev-actions">
              <p className="lp-login-dev-hint">
                Dev: <code>test@gmail.com</code> / <code>password123</code> · API :8001
              </p>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--block" onClick={enterDevPreview}>
                Preview workspace (UI only)
              </button>
            </div>
          )}

          <p className="lp-login-footnote">
            Protected by DataFlow control-plane auth · TLS in transit · operator audit on sign-in
          </p>
        </div>
      </section>
    </main>
  );
}

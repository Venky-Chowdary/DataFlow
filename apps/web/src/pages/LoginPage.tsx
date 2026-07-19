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

const TRUST_POINTS = [
  { title: "Semantic mapping", body: "Reviewable confidence — never silent remap." },
  { title: "Preflight gates", body: "Eight checks before any production write." },
  { title: "Proof & reconcile", body: "Job Theater evidence after every run." },
  { title: "Enterprise control", body: "SSO, RBAC, and audit-ready trails." },
];

export function LoginPage({ target, onAuthenticated, onBack }: LoginPageProps) {
  const { toast } = useToast();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [remember, setRemember] = useState(true);
  const [submitted, setSubmitted] = useState(false);
  const [checking, setChecking] = useState(false);
  const [credentialError, setCredentialError] = useState("");
  const [capsLock, setCapsLock] = useState(false);
  const [ssoProviders, setSsoProviders] = useState<Array<{ type: SsoType; label: string; login_path: string }>>([]);

  useEffect(() => {
    fetchSsoProviders().then(setSsoProviders).catch(() => setSsoProviders([]));
  }, []);

  const emailTrimmed = email.trim();
  const emailError = submitted && !isValidEmail(email) ? "Enter a valid work email (name@company.com)." : "";
  const passwordError =
    submitted && password.length === 0
      ? "Password is required."
      : submitted && password.length < 8
        ? "Password must be at least 8 characters."
        : "";
  const targetLabel = TARGET_LABELS[target] ?? "DataFlow";
  const ready = useMemo(() => isValidEmail(email) && password.length >= 8, [email, password]);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setSubmitted(true);
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
        // Surface the real API detail (password `$` hint, missing users, etc.)
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

  return (
    <main className="lp-login lp-login--enterprise">
      <div className="lp-login-atmosphere" aria-hidden>
        <span className="lp-login-grid" />
        <span className="lp-login-glow lp-login-glow--a" />
        <span className="lp-login-glow lp-login-glow--b" />
      </div>

      <header className="lp-login-topbar">
        <button type="button" className="lp-login-back" onClick={onBack}>
          <DtIcon name="chevron-left" size={15} /> Back
        </button>
        <a
          className="lp-login-topbar-brand"
          href="#/"
          onClick={(e) => {
            e.preventDefault();
            onBack();
          }}
        >
          <DtLogo size={26} />
          <span>DataFlow</span>
        </a>
        <span className="lp-login-topbar-meta">Secure workspace</span>
      </header>

      <div className="lp-login-layout">
        <aside className="lp-login-aside" aria-label="DataFlow platform">
          <p className="lp-login-aside-kicker">Universal data platform</p>
          <h1 className="lp-login-aside-title">
            <span className="lp-login-brand-mark">DataFlow</span>
            <span className="lp-login-aside-headline">Sign in to governed movement</span>
          </h1>
          <p className="lp-login-aside-lead">
            Continue to <strong>{targetLabel}</strong> with the same engine behind Transfer Studio, Job Theater, and Data Pilot.
          </p>
          <ul className="lp-login-trust lp-login-trust--cards">
            {TRUST_POINTS.map((point) => (
              <li key={point.title}>
                <DtIcon name="check" size={16} />
                <div>
                  <strong>{point.title}</strong>
                  <span>{point.body}</span>
                </div>
              </li>
            ))}
          </ul>
        </aside>

        <section className="lp-login-panel" aria-labelledby="login-form-title">
          <div className="lp-login-card">
            <div className="lp-login-card-head">
              <h2 id="login-form-title">Sign in</h2>
              <p className="lp-login-sub">Use your workspace email and password.</p>
            </div>

            {credentialError && (
              <div
                className={`lp-login-alert ${credentialError.toLowerCase().includes("api") || credentialError.toLowerCase().includes("configured") ? "" : "lp-login-alert--warn"}`}
                role="alert"
              >
                <DtIcon name="alert" size={18} />
                <div>
                  <strong>
                    {credentialError.toLowerCase().includes("api")
                      ? "Control plane unreachable"
                      : credentialError.toLowerCase().includes("configured")
                        ? "Workspace not ready"
                        : "Sign-in failed"}
                  </strong>
                  <p>{credentialError}</p>
                </div>
              </div>
            )}

            <form className="lp-login-form" onSubmit={submit} noValidate>
              <div className={`lp-field ${emailError ? "is-error" : ""}`}>
                <label className="lp-label" htmlFor="login-email">Work email</label>
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

              <div className={`lp-field ${passwordError ? "is-error" : ""}`}>
                <div className="lp-label-row">
                  <label className="lp-label" htmlFor="login-password">Password</label>
                  {capsLock && <span className="lp-login-caps">Caps Lock is on</span>}
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
                    onKeyUp={(e) => setCapsLock(e.getModifierState?.("CapsLock") ?? false)}
                    onKeyDown={(e) => setCapsLock(e.getModifierState?.("CapsLock") ?? false)}
                    autoComplete="current-password"
                    placeholder="At least 8 characters"
                    aria-invalid={Boolean(passwordError || (credentialError && !credentialError.toLowerCase().includes("api")))}
                    aria-describedby={passwordError ? "login-password-error" : undefined}
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
                {passwordError && <small id="login-password-error" className="lp-field-error">{passwordError}</small>}
              </div>

              <label className="lp-login-remember">
                <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
                <span>Keep me signed in on this device</span>
              </label>

              <button
                type="submit"
                className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block lp-login-submit"
                disabled={checking}
              >
                {checking ? "Signing in…" : "Sign in to workspace"}
              </button>
            </form>

            {import.meta.env.DEV && (
              <div className="lp-login-dev-actions">
                <p className="lp-login-dev-hint">
                  Dev credentials: <code>test@gmail.com</code> / <code>password123</code> — requires API on port 8001.
                </p>
                <button type="button" className="lp-btn lp-btn--outline lp-btn--block" onClick={enterDevPreview}>
                  Preview workspace (UI only)
                </button>
              </div>
            )}

            {ssoProviders.length > 0 && (
              <>
                <div className="lp-login-divider"><span>Enterprise SSO</span></div>
                <div className="lp-login-sso">
                  {ssoProviders.map((provider) => (
                    <button
                      key={provider.type}
                      type="button"
                      className="lp-btn lp-btn--outline"
                      onClick={() => startSso(provider.type, provider.label)}
                    >
                      {provider.label}
                    </button>
                  ))}
                </div>
              </>
            )}

            <p className="lp-login-footnote">
              Protected session · credentials verified by the DataFlow API · never stored in the browser as plain text
            </p>
          </div>
        </section>
      </div>
    </main>
  );
}

import { FormEvent, useMemo, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { useToast } from "../components/Toast";
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

export function LoginPage({ target, onAuthenticated, onBack }: LoginPageProps) {
  const { toast } = useToast();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [remember, setRemember] = useState(true);
  const [submitted, setSubmitted] = useState(false);

  const emailError = submitted && !isValidEmail(email) ? "Enter a valid work email." : "";
  const passwordError = submitted && password.length < 8 ? "Use at least 8 characters." : "";
  const targetLabel = TARGET_LABELS[target] ?? "DataFlow";
  const ready = useMemo(() => isValidEmail(email) && password.length >= 8, [email, password]);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    setSubmitted(true);
    if (!ready) {
      toast({
        title: "Sign-in details need attention",
        message: "Use a valid work email and a password with at least 8 characters.",
        tone: "warning",
      });
      return;
    }

    try {
      if (remember) {
        localStorage.setItem("df2.session", JSON.stringify({ email: email.trim(), signed_in_at: Date.now() }));
      } else {
        sessionStorage.setItem("df2.session", JSON.stringify({ email: email.trim(), signed_in_at: Date.now() }));
      }
    } catch {
      // Storage can be unavailable in private contexts; the in-memory session still proceeds.
    }
    toast({ title: "Signed in", message: `Opening ${targetLabel}.`, tone: "success" });
    onAuthenticated(email.trim());
  };

  const ssoComingSoon = (provider: string) => {
    toast({
      title: `${provider} SSO not connected yet`,
      message: "The sign-in surface is ready; backend OIDC/SAML enforcement still needs to be wired.",
      tone: "info",
    });
  };

  return (
    <main className="df2-login-page">
      <section className="df2-login-visual" aria-label="DataFlow security posture">
        <button type="button" className="df2-login-back" onClick={onBack}>
          <DtIcon name="transfer" size={15} /> Back
        </button>
        <div className="df2-login-brand">
          <span className="df2-login-mark"><DtIcon name="shield" size={24} /></span>
          <div>
            <strong>DataFlow</strong>
            <span>Enterprise control plane</span>
          </div>
        </div>
        <div className="df2-login-copy">
          <span className="df2-page-kicker"><DtIcon name="gate" size={14} /> Secure workspace</span>
          <h1>Sign in before touching production data.</h1>
          <p>
            Access is gated before transfers, connector secrets, job history, and schema policies are exposed.
          </p>
        </div>
        <div className="df2-login-proof-grid">
          <div><span>Session</span><strong>Workspace scoped</strong></div>
          <div><span>Secrets</span><strong>Hidden by default</strong></div>
          <div><span>Audit</span><strong>Event ready</strong></div>
          <div><span>Target</span><strong>{targetLabel}</strong></div>
        </div>
      </section>

      <section className="df2-login-panel" aria-label="Sign in">
        <div className="df2-login-card">
          <div className="df2-login-head">
            <span className="df2-rail-kicker">Workspace sign in</span>
            <h2>Continue to {targetLabel}</h2>
            <p>Use a work identity. SSO buttons are prepared for the backend identity provider.</p>
          </div>

          <form className="df2-login-form" onSubmit={submit} noValidate>
            <div className={`df2-field ${emailError ? "df2-field-error" : ""}`}>
              <label className="df2-label" htmlFor="login-email">Work email</label>
              <input
                id="login-email"
                className="df2-input"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                autoComplete="email"
                placeholder="you@company.com"
              />
              {emailError && <small>{emailError}</small>}
            </div>

            <div className={`df2-field ${passwordError ? "df2-field-error" : ""}`}>
              <label className="df2-label" htmlFor="login-password">Password</label>
              <input
                id="login-password"
                className="df2-input"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete="current-password"
                placeholder="At least 8 characters"
              />
              {passwordError && <small>{passwordError}</small>}
            </div>

            <label className="df2-login-remember">
              <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
              <span>Keep me signed in on this workspace</span>
            </label>

            <button type="submit" className="df2-btn df2-btn-primary df2-btn-lg df2-btn-block">
              <DtIcon name="shield" size={16} /> Sign in
            </button>
          </form>

          <div className="df2-login-divider"><span>Enterprise SSO</span></div>
          <div className="df2-login-sso">
            <button type="button" className="df2-btn" onClick={() => ssoComingSoon("SAML")}>SAML</button>
            <button type="button" className="df2-btn" onClick={() => ssoComingSoon("OIDC")}>OIDC</button>
            <button type="button" className="df2-btn" onClick={() => ssoComingSoon("Google Workspace")}>Google</button>
          </div>
        </div>
      </section>
    </main>
  );
}

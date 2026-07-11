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

export function LoginPage({ target, onAuthenticated, onBack }: LoginPageProps) {
  const { toast } = useToast();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [remember, setRemember] = useState(true);
  const [submitted, setSubmitted] = useState(false);
  const [checking, setChecking] = useState(false);
  const [credentialError, setCredentialError] = useState("");
  const [ssoProviders, setSsoProviders] = useState<Array<{ type: SsoType; label: string; login_path: string }>>([]);

  useEffect(() => {
    fetchSsoProviders().then(setSsoProviders).catch(() => setSsoProviders([]));
  }, []);

  const emailError = submitted && !isValidEmail(email) ? "Enter a valid work email." : "";
  const passwordError = submitted && password.length < 8 ? "Use at least 8 characters." : "";
  const targetLabel = TARGET_LABELS[target] ?? "DataFlow";
  const ready = useMemo(() => isValidEmail(email) && password.length >= 8, [email, password]);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setSubmitted(true);
    setCredentialError("");
    if (!ready) {
      toast({
        title: "Sign-in details need attention",
        message: "Use a valid work email and a password with at least 8 characters.",
        tone: "warning",
      });
      return;
    }

    setChecking(true);
    try {
      const result = await loginWorkspace(email, password);
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
        msg.includes("fetch");
      if (apiOffline) {
        setCredentialError("Cannot reach the API. Verify the server is running and VITE_API_BASE is correct.");
        toast({
          title: "API offline",
          message: "The control plane is not running. Start the API server and try again.",
          tone: "error",
        });
      } else {
        setCredentialError("Email or password is incorrect.");
        toast({
          title: "Sign-in failed",
          message: "Check your credentials and try again.",
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

  return (
    <main className="lp-login">
      <span className="lp-login-orb lp-login-orb--1" aria-hidden />
      <span className="lp-login-orb lp-login-orb--2" aria-hidden />

      <header className="lp-login-header">
        <button type="button" className="lp-login-back" onClick={onBack}>
          <DtIcon name="transfer" size={15} /> Back
        </button>
      </header>

      <div className="lp-login-body">
        <div className="lp-login-card">
          <div className="lp-login-brand">
            <DtLogo size={40} />
            <div className="lp-login-brand-text">
              <strong>DataFlow</strong>
              <span>Enterprise workspace</span>
            </div>
          </div>

          <h1>Sign in to {targetLabel}</h1>
          <p className="lp-login-sub">Server-verified access to transfers, connectors, and job history.</p>

          {credentialError && credentialError.includes("API") && (
            <div className="df2-alert df2-alert-error" role="alert">
              <DtIcon name="alert" size={18} />
              <div>
                <strong>Control plane unreachable</strong>
                <p>{credentialError}</p>
              </div>
            </div>
          )}

          <form className="lp-login-form" onSubmit={submit} noValidate>
            <div className={`df2-field ${emailError ? "df2-field-error" : ""}`}>
              <label className="df2-label" htmlFor="login-email">Work email</label>
              <input
                id="login-email"
                className="df2-input"
                type="email"
                value={email}
                onChange={(e) => { setEmail(e.target.value); setCredentialError(""); }}
                autoComplete="email"
                placeholder="you@company.com"
              />
              {emailError && <small>{emailError}</small>}
            </div>

            <div className={`df2-field ${passwordError || credentialError ? "df2-field-error" : ""}`}>
              <label className="df2-label" htmlFor="login-password">Password</label>
              <input
                id="login-password"
                className="df2-input"
                type="password"
                value={password}
                onChange={(e) => { setPassword(e.target.value); setCredentialError(""); }}
                autoComplete="current-password"
                placeholder="At least 8 characters"
              />
              {passwordError && <small>{passwordError}</small>}
              {!passwordError && credentialError && <small>{credentialError}</small>}
            </div>

            <label className="lp-login-remember">
              <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
              <span>Keep me signed in</span>
            </label>

            <button type="submit" className="df2-btn df2-btn-primary df2-btn-lg df2-btn-block" disabled={checking}>
              <DtIcon name="shield" size={16} /> {checking ? "Signing in…" : "Sign in"}
            </button>
          </form>

          {ssoProviders.length > 0 && (
            <>
              <div className="lp-login-divider"><span>Enterprise SSO</span></div>
              <div className="lp-login-sso">
                {ssoProviders.map((provider) => (
                  <button key={provider.type} type="button" className="df2-btn" onClick={() => startSso(provider.type, provider.label)}>
                    {provider.label}
                  </button>
                ))}
              </div>
            </>
          )}
        </div>
      </div>
    </main>
  );
}

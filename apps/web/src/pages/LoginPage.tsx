import { FormEvent, useEffect, useMemo, useState } from "react";
import { AuthBrandPanel } from "../components/AuthBrandPanel";
import { DtIcon } from "../components/DtIcon";
import { AuthButton } from "../components/AuthButton";
import { PasswordToggle } from "../components/AuthFlow";
import { useToast } from "../components/Toast";
import { ApiError, fetchSsoProviders, loginWorkspace, ssoStartUrl, SsoType, type LoginSession } from "../lib/api";
import { isMfaChallenge } from "../lib/mfaApi";
import { LoginMfaStep } from "../components/MfaChallenge";
import { fetchSignupConfig } from "../lib/signupApi";
import { classifySignInFailure, SignInFailure } from "../lib/signInFailure";
import { UI_PREVIEW_TOKEN, writeSession } from "../lib/session";
import { Screen } from "../lib/types";
import type { PublicRoute } from "../lib/publicNavigation";

interface LoginPageProps {
  target: Screen;
  onAuthenticated: (email: string) => void;
  onBack: () => void;
  onLegal?: (route: Extract<PublicRoute, "privacy" | "terms">) => void;
  /** Self-serve signup (flag-gated server-side; links render only when the
   *  deployment answers the form-token route). */
  onSignup?: () => void;
  onForgotPassword?: () => void;
  /** 403 email_unverified: the API minted a fresh code — jump into code entry. */
  onVerifyPending?: (challengeId: string, email: string) => void;
}

function isValidEmail(value: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());
}

const TARGET_LABELS: Partial<Record<Screen, string>> = {
  dashboard: "Overview",
  transfer: "Transfer Studio",
  pilot: "Datawrap Pilot",
  connectors: "Connectors",
  jobs: "Job Theater",
  settings: "Settings",
};

const TRUST_METRICS = [
  { value: "9", label: "Preflight gates" },
  { value: "Map", label: "Semantic confidence" },
  { value: "Σ", label: "Checksum proof" },
];

export function LoginPage({ target, onAuthenticated, onBack, onLegal, onSignup, onForgotPassword, onVerifyPending }: LoginPageProps) {
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
  // Which failure happened, taken from the response — not guessed from words in
  // the message. Sniffing for "api" reported a rejected password as an
  // unreachable control plane, sending the operator to check the deployment.
  const [failure, setFailure] = useState<SignInFailure>("auth");
  const [capsLock, setCapsLock] = useState(false);
  const [ssoProviders, setSsoProviders] = useState<Array<{ type: SsoType; label: string; login_path: string }>>([]);
  const [signupEnabled, setSignupEnabled] = useState(false);
  const [pendingVerify, setPendingVerify] = useState<{ challengeId: string; email: string } | null>(null);
  // Password matched an account with two-factor on: the second step is pending.
  const [mfaStep, setMfaStep] = useState<{ challenge: string; email: string } | null>(null);

  useEffect(() => {
    fetchSsoProviders().then(setSsoProviders).catch(() => setSsoProviders([]));
    // Read-only capability probe on the always-mounted auth router — never
    // mints a form token just to feature-detect the signup surface.
    fetchSignupConfig().then((c) => setSignupEnabled(c.enabled));
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

  const targetLabel = TARGET_LABELS[target] ?? "Datawrap";
  const emailOk = isValidEmail(email);
  const passwordOk = password.length >= 8;
  const ready = useMemo(() => emailOk && passwordOk, [emailOk, passwordOk]);

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
      if (isMfaChallenge(result)) {
        setPassword("");
        setMfaStep({ challenge: result.challenge, email: emailTrimmed });
        return;
      }
      finishSignIn(result);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "";
      const kind = classifySignInFailure(err);
      setFailure(kind);
      if (err instanceof ApiError && err.code === "email_unverified" && err.challengeId) {
        setPendingVerify({ challengeId: err.challengeId, email: emailTrimmed });
        setCredentialError("Your email isn't verified yet — we sent a fresh code.");
      } else if (kind === "api") {
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

  const finishSignIn = (result: LoginSession & { recovery_codes_remaining?: number; mfa_method?: string }) => {
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
    if (result.mfa_method === "recovery") {
      toast({
        title: "Signed in with a recovery code",
        message: `${result.recovery_codes_remaining ?? 0} recovery codes left. Create new ones in Settings → Security.`,
        tone: "warning",
      });
    } else {
      toast({ title: "Signed in", message: `Opening ${targetLabel}.`, tone: "success" });
    }
    onAuthenticated(result.user.email);
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
        token: UI_PREVIEW_TOKEN,
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

  const alertKind = failure;

  return (
    <main className="lp-login lp-login--gate">
      <AuthBrandPanel
        title="Sign in to governed transfers"
        lead="Semantic mapping, named preflight gates, and checksum proof — for humans and agents."
        onBack={onBack}
        metrics={TRUST_METRICS}
        dest={<>Continue to <strong>{targetLabel}</strong></>}
      />

      <section className="lp-login-auth" aria-labelledby="login-form-title">
        <div className="lp-login-auth-bg" aria-hidden>
          <span className="lp-login-auth-glow lp-login-auth-glow--a" />
          <span className="lp-login-auth-glow lp-login-auth-glow--b" />
          <span className="lp-login-auth-grid" />
          <span className="lp-login-auth-arc" />
        </div>

        <div className="lp-login-auth-inner">
            <div className="lp-login-auth-card">
            {mfaStep ? (
              <LoginMfaStep
                challenge={mfaStep.challenge}
                email={mfaStep.email}
                onSignedIn={finishSignIn}
                onBack={() => setMfaStep(null)}
              />
            ) : (<>
            <div className="lp-login-auth-head">
              <p className="lp-login-auth-kicker">Secure workspace access</p>
              <h2 id="login-form-title">Welcome back</h2>
              <p className="lp-login-auth-sub">
                Sign in to open <strong>{targetLabel}</strong> — same map → validate → move → prove path.
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

            {pendingVerify && (
              <UnverifiedAlert
                challengeId={pendingVerify.challengeId}
                email={pendingVerify.email}
                error={credentialError}
                onVerifyPending={onVerifyPending}
              />
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
                  />
                  <PasswordToggle show={showPassword} onToggle={() => setShowPassword((v) => !v)} />
                </div>
                {passwordError && <small className="lp-field-error">{passwordError}</small>}
              </div>

              <LoginAuxRows
                remember={remember}
                onRemember={setRemember}
                showForgot={Boolean(signupEnabled && onForgotPassword)}
                onForgot={onForgotPassword}
              />

              <AuthButton
                type="submit" size="lg" block
                className="lp-login-submit"
                loading={checking} loadingLabel="Verifying credentials…"
                trailingIcon={<DtIcon name="arrow-right" size={16} />}
              >
                Sign in to workspace
              </AuthButton>
            </form>

            {signupEnabled && onSignup && (
              <p className="lp-login-legal">
                New to Datawrap? <AuthButton variant="link" onClick={onSignup}>Create an account</AuthButton>
              </p>
            )}

            {ssoProviders.length > 0 && (
              <>
                <div className="lp-login-divider"><span>Or continue with SSO</span></div>
                <div className="lp-login-sso">
                  {ssoProviders.map((provider) => (
                    <AuthButton
                      key={provider.type}
                      variant="secondary"
                      className="lp-login-sso-btn"
                      onClick={() => startSso(provider.type, provider.label)}
                    >
                      {provider.label}
                    </AuthButton>
                  ))}
                </div>
              </>
            )}

            {import.meta.env.DEV && (
              <div className="lp-login-dev-actions">
                <p className="lp-login-dev-hint">
                  Dev: <code>test@gmail.com</code> / <code>password123</code> · API :8001
                </p>
                <AuthButton variant="secondary" block onClick={enterDevPreview}>
                  Preview workspace (UI only)
                </AuthButton>
              </div>
            )}

            <ul className="lp-login-trust" aria-label="Session guarantees">
              <li>
                <DtIcon name="lock" size={14} />
                <span>TLS session</span>
              </li>
              <li>
                <DtIcon name="shield" size={14} />
                <span>RBAC enforced</span>
              </li>
              <li>
                <DtIcon name="scan" size={14} />
                <span>Sign-in audited</span>
              </li>
            </ul>
            </>)}
            {onLegal ? (
              <p className="lp-login-legal">
                <AuthButton variant="link" onClick={() => onLegal("privacy")}>Privacy</AuthButton>
                <span aria-hidden>·</span>
                <AuthButton variant="link" onClick={() => onLegal("terms")}>Terms</AuthButton>
              </p>
            ) : null}
          </div>
        </div>
      </section>
    </main>
  );
}

/**
 * The 403 `email_unverified` alert — the API minted a fresh code, so the
 * action jumps straight into code entry. The control is a styled link
 * button (AuthButton variant="link"), never a raw unstyled <button>.
 */
export function UnverifiedAlert({
  challengeId,
  email,
  error,
  onVerifyPending,
}: {
  challengeId: string;
  email: string;
  error: string | null;
  onVerifyPending?: (challengeId: string, email: string) => void;
}) {
  return (
    <div className="lp-login-alert lp-login-alert--warn" role="alert">
      <DtIcon name="alert" size={18} />
      <div>
        <strong>Verify your email</strong>
        <p>
          {error}{" "}
          {onVerifyPending && (
            <AuthButton
              variant="link"
              size="sm"
              onClick={() => onVerifyPending(challengeId, email)}
            >
              Enter verification code
            </AuthButton>
          )}
        </p>
      </div>
    </div>
  );
}

/**
 * The forgot-password link and the remember-me checkbox each get their own
 * line: the link sits right-aligned directly under the password field, the
 * checkbox follows below — they never share a row (the checkbox copy would
 * wrap to two lines).
 */
export function LoginAuxRows({
  remember,
  onRemember,
  showForgot,
  onForgot,
}: {
  remember: boolean;
  onRemember: (v: boolean) => void;
  showForgot: boolean;
  onForgot?: () => void;
}) {
  return (
    <>
      {showForgot && onForgot && (
        <div className="lp-login-forgot-row">
          <AuthButton variant="link" className="lp-login-forgot" onClick={onForgot}>
            Forgot password?
          </AuthButton>
        </div>
      )}
      <div className="lp-login-row">
        <label className="lp-login-remember">
          <input type="checkbox" checked={remember} onChange={(e) => onRemember(e.target.checked)} />
          <span>Keep me signed in on this device</span>
        </label>
      </div>
    </>
  );
}

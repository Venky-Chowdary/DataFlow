import { FormEvent, ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { AuthBrandPanel } from "../components/AuthBrandPanel";
import { DtIcon } from "../components/DtIcon";
import { useToast } from "../components/Toast";
import {
  fetchFormToken,
  fetchSignupConfig,
  forgotPassword,
  resendCode,
  resetPassword,
  routeSsoForEmail,
  SignupApiError,
  signupRequest,
  verifyEmail,
} from "../lib/signupApi";
import { changeEmailRequestError, codeEntryError, mailSendError } from "../lib/authFlowCopy";
import { focusFirstInvalid, ResendRow } from "../components/AuthFlow";
import { writeSession } from "../lib/session";

function isValidEmail(value: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());
}

interface AuthFlowProps {
  onAuthenticated: (email: string) => void;
  onSignIn: () => void;
  onBack: () => void;
}

/** Shared two-column chrome: brand rail left, form card right.
 *  Back lives on the dark brand header — never as a pill on the card. */
function AuthShell({ title, sub, onBack, children }: { title: string; sub: string; onBack: () => void; children: ReactNode }) {
  return (
    <main className="lp-login lp-login--gate lp-login--flow">
      <AuthBrandPanel title={title} lead={sub} onBack={onBack} />
      <section className="lp-login-auth">
        <div className="lp-login-auth-bg" aria-hidden>
          <span className="lp-login-auth-glow lp-login-auth-glow--a" />
          <span className="lp-login-auth-glow lp-login-auth-glow--b" />
          <span className="lp-login-auth-grid" />
          <span className="lp-login-auth-arc" />
        </div>
        <div className="lp-login-auth-inner">
          <div className="lp-login-auth-card">{children}</div>
        </div>
      </section>
    </main>
  );
}

/** Cloudflare Turnstile — renders only when the deployment publishes a
 *  site key; no-ops to invisible otherwise. */
function TurnstileWidget({ siteKey, onToken }: { siteKey: string; onToken: (token: string) => void }) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const tokenRef = useRef(onToken);
  tokenRef.current = onToken;

  useEffect(() => {
    if (!siteKey) return;
    const render = () => {
      const turnstile = (window as unknown as { turnstile?: { render: (el: HTMLElement, opts: Record<string, unknown>) => void } }).turnstile;
      if (hostRef.current && turnstile) {
        turnstile.render(hostRef.current, {
          sitekey: siteKey,
          callback: (token: string) => tokenRef.current(token),
          "expired-callback": () => tokenRef.current(""),
        });
      }
    };
    const existing = document.querySelector<HTMLScriptElement>('script[data-df-turnstile="1"]');
    if (existing) {
      if ((window as unknown as { turnstile?: unknown }).turnstile) render();
      else existing.addEventListener("load", render, { once: true });
      return;
    }
    const script = document.createElement("script");
    script.src = "https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit";
    script.async = true;
    script.defer = true;
    script.dataset.dfTurnstile = "1";
    script.addEventListener("load", render, { once: true });
    document.head.appendChild(script);
  }, [siteKey]);

  if (!siteKey) return null;
  return <div ref={hostRef} className="lp-login-turnstile" data-testid="turnstile-widget" />;
}

/** Six-digit code entry. */
function CodeStep({
  email,
  challengeId,
  resendAfterSec,
  onSubmitCode,
  busy,
  error,
  errorIsField,
}: {
  email: string;
  challengeId: string;
  resendAfterSec: number;
  onSubmitCode: (code: string) => void;
  busy: boolean;
  error: string;
  errorIsField: boolean;
}) {
  const [code, setCode] = useState("");
  const cardRef = useRef<HTMLDivElement | null>(null);
  const fieldError = errorIsField ? error : "";

  return (
    <div ref={cardRef}>
      <div className="lp-login-auth-head">
        <p className="lp-login-auth-kicker">Check your inbox</p>
        <h2>Enter the 6-digit code</h2>
        <p className="lp-login-auth-sub">
          We sent a verification code to <strong>{email}</strong>. It expires in 10 minutes.
        </p>
      </div>
      {error && !errorIsField && (
        <div className="lp-login-alert lp-login-alert--warn" role="alert">
          <DtIcon name="alert" size={18} />
          <div><strong>Couldn't verify that code</strong><p>{error}</p></div>
        </div>
      )}
      <form
        className="lp-login-form"
        onSubmit={(e) => {
          e.preventDefault();
          if (code.trim().length < 6) { focusFirstInvalid(cardRef.current); return; }
          onSubmitCode(code.trim());
        }}
        noValidate
      >
        <div className={`lp-field ${fieldError ? "is-error" : ""}`}>
          <label className="lp-label" htmlFor="auth-code">Verification code</label>
          <input
            id="auth-code"
            className="lp-input"
            inputMode="numeric"
            autoComplete="one-time-code"
            maxLength={6}
            value={code}
            onChange={(e) => setCode(e.target.value.replace(/\D/g, "").slice(0, 6))}
            placeholder="000000"
            autoFocus
            aria-invalid={Boolean(fieldError)}
            aria-describedby={fieldError ? "auth-code-error" : undefined}
          />
          {fieldError && <small id="auth-code-error" className="lp-field-error">{fieldError}</small>}
        </div>
        <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block lp-login-submit" disabled={busy || code.length < 6}>
          {busy ? "Verifying…" : "Verify email"}
          {!busy && <DtIcon name="arrow-right" size={16} />}
        </button>
        <ResendRow challengeId={challengeId} initialCooldown={resendAfterSec} />
      </form>
    </div>
  );
}

export function SignupPage({ onAuthenticated, onSignIn, onBack, initialEmail = "", initialChallengeId = "" }: AuthFlowProps & { initialEmail?: string; initialChallengeId?: string }) {
  const { toast } = useToast();
  const [step, setStep] = useState<"details" | "verify">(initialChallengeId ? "verify" : "details");
  const [email, setEmail] = useState(initialEmail);
  const [name, setName] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [errorIsField, setErrorIsField] = useState(false);
  const [formToken, setFormToken] = useState("");
  const [turnstileSiteKey, setTurnstileSiteKey] = useState("");
  const [turnstileToken, setTurnstileToken] = useState("");
  const [challenge, setChallenge] = useState({ id: initialChallengeId, resendAfterSec: 60 });
  const [ssoHint, setSsoHint] = useState<{ sso: string; startUrl: string } | null>(null);
  const cardRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    fetchFormToken("signup").then((t) => setFormToken(t ?? ""));
    fetchSignupConfig().then((c) => setTurnstileSiteKey(c.turnstileSiteKey));
  }, []);

  const emailOk = isValidEmail(email);
  const passwordOk = password.length >= 12;
  const nameOk = name.trim().length >= 2;
  const ready = useMemo(() => emailOk && passwordOk && nameOk, [emailOk, passwordOk, nameOk]);

  const emailError = submitted && !emailOk ? "Enter a valid work email." : "";
  const nameError = submitted && !nameOk ? "Your name is required." : "";
  const passwordError = submitted && !passwordOk ? "Use at least 12 characters." : "";

  useEffect(() => {
    if (!emailOk) { setSsoHint(null); return; }
    const t = window.setTimeout(() => {
      routeSsoForEmail(email.trim()).then((r) => {
        setSsoHint(r.sso && r.start_url ? { sso: r.sso, startUrl: r.start_url } : null);
      });
    }, 400);
    return () => window.clearTimeout(t);
  }, [email, emailOk]);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitted(true);
    setError("");
    if (!ready) { focusFirstInvalid(cardRef.current); return; }
    setBusy(true);
    try {
      const r = await signupRequest({
        email: email.trim(),
        name: name.trim(),
        password,
        formToken: formToken || undefined,
        turnstileToken: turnstileToken || undefined,
      });
      setChallenge({ id: r.challengeId, resendAfterSec: r.resendAfterSec });
      setStep("verify");
    } catch (err) {
      setError(mailSendError(
        err instanceof SignupApiError ? err : {},
        err instanceof Error ? err.message : "Signup failed. Try again.",
      ));
      toast({ title: "Couldn't start signup", message: "Check the fields and try again.", tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const submitCode = async (code: string) => {
    setBusy(true);
    setError("");
    setErrorIsField(false);
    try {
      const result = await verifyEmail({ challengeId: challenge.id, code });
      writeSession(
        {
          email: result.user.email,
          name: result.user.name ?? "",
          role: result.user.role ?? "admin",
          token: "", // cookie session carries auth; no bearer on the new surface
          expires_at: Math.floor(Date.now() / 1000) + 86400,
          signed_in_at: Date.now(),
        },
        true,
      );
      toast({ title: "Email verified", message: "Your workspace is ready.", tone: "success" });
      onAuthenticated(result.user.email);
    } catch (err) {
      setError(codeEntryError(
        err instanceof SignupApiError ? err : { message: err instanceof Error ? err.message : "" },
        "Couldn't verify that code. Try again.",
      ));
      setErrorIsField(err instanceof SignupApiError && err.code === "invalid_code");
    } finally {
      setBusy(false);
    }
  };

  if (step === "verify") {
    return (
      <AuthShell title="Verify your email" sub="One step left — confirm the address owns this workspace." onBack={onBack}>
        <CodeStep
          email={email.trim()}
          challengeId={challenge.id}
          resendAfterSec={challenge.resendAfterSec}
          onSubmitCode={submitCode}
          busy={busy}
          error={error}
          errorIsField={errorIsField}
        />
      </AuthShell>
    );
  }

  return (
    <AuthShell title="Create your workspace" sub="Verified email, governed transfers, and checksum proof — in minutes." onBack={onBack}>
      <div ref={cardRef}>
        <div className="lp-login-auth-head">
          <p className="lp-login-auth-kicker">Self-serve signup</p>
          <h2>Get started</h2>
          <p className="lp-login-auth-sub">
            We'll email you a 6-digit code to verify your address.
          </p>
        </div>

        {ssoHint && (
          <div className="lp-login-alert lp-login-alert--warn" role="status">
            <DtIcon name="shield" size={18} />
            <div>
              <strong>Your organization uses SSO</strong>
              <p>
                Sign in through your identity provider instead.{" "}
                <a href={ssoHint.startUrl}>Continue with {ssoHint.sso.toUpperCase()}</a>
              </p>
            </div>
          </div>
        )}

        {error && (
          <div className="lp-login-alert lp-login-alert--danger" role="alert">
            <DtIcon name="alert" size={18} />
            <div><strong>Couldn't start signup</strong><p>{error}</p></div>
          </div>
        )}

        <form className="lp-login-form" onSubmit={submit} noValidate>
          <div className={`lp-field ${nameError ? "is-error" : ""}`}>
            <label className="lp-label" htmlFor="signup-name">Full name</label>
            <input
              id="signup-name"
              className="lp-input"
              value={name}
              onChange={(e) => setName(e.target.value)}
              autoComplete="name"
              placeholder="Ada Lovelace"
              aria-invalid={Boolean(nameError)}
              aria-describedby={nameError ? "signup-name-error" : undefined}
            />
            {nameError && <small id="signup-name-error" className="lp-field-error">{nameError}</small>}
          </div>

          <div className={`lp-field ${emailError ? "is-error" : emailOk ? "is-ok" : ""}`}>
            <div className="lp-label-row">
              <label className="lp-label" htmlFor="signup-email">Work email</label>
              {emailOk && <span className="lp-field-status lp-field-status--ok">Valid</span>}
            </div>
            <input
              id="signup-email"
              className="lp-input"
              type="email"
              inputMode="email"
              value={email}
              onChange={(e) => { setEmail(e.target.value); setError(""); }}
              autoComplete="username"
              autoCapitalize="none"
              autoCorrect="off"
              spellCheck={false}
              placeholder="you@company.com"
              aria-invalid={Boolean(emailError)}
              aria-describedby={emailError ? "signup-email-error" : undefined}
            />
            {emailError && <small id="signup-email-error" className="lp-field-error">{emailError}</small>}
          </div>

          <div className={`lp-field ${passwordError ? "is-error" : passwordOk ? "is-ok" : ""}`}>
            <label className="lp-label" htmlFor="signup-password">Password</label>
            <div className="lp-input-password">
              <input
                id="signup-password"
                className="lp-input"
                type={showPassword ? "text" : "password"}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete="new-password"
                placeholder="At least 12 characters"
                aria-invalid={Boolean(passwordError)}
                aria-describedby={passwordError ? "signup-password-error" : undefined}
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
            {passwordError && <small id="signup-password-error" className="lp-field-error">{passwordError}</small>}
          </div>

          <TurnstileWidget siteKey={turnstileSiteKey} onToken={setTurnstileToken} />

          <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block lp-login-submit" disabled={busy}>
            {busy ? "Sending code…" : "Send verification code"}
            {!busy && <DtIcon name="arrow-right" size={16} />}
          </button>
        </form>

        <p className="lp-login-legal">
          Already have an account? <button type="button" onClick={onSignIn}>Sign in</button>
        </p>
      </div>
    </AuthShell>
  );
}

export function ForgotPasswordPage({ onSignIn, onBack }: { onSignIn: () => void; onBack: () => void }) {
  const { toast } = useToast();
  const [step, setStep] = useState<"email" | "reset" | "done">("email");
  const [email, setEmail] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [formToken, setFormToken] = useState("");
  const [turnstileSiteKey, setTurnstileSiteKey] = useState("");
  const [turnstileToken, setTurnstileToken] = useState("");
  const [challenge, setChallenge] = useState({ id: "", resendAfterSec: 60 });
  const [code, setCode] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [codeServerError, setCodeServerError] = useState("");
  const cardRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    fetchFormToken("reset").then((t) => setFormToken(t ?? ""));
    fetchSignupConfig().then((c) => setTurnstileSiteKey(c.turnstileSiteKey));
  }, []);

  const emailOk = isValidEmail(email);
  const codeOk = code.trim().length === 6;
  const passwordOk = newPassword.length >= 12;
  const emailError = submitted && !emailOk ? "Enter a valid work email." : "";
  const codeError = codeServerError || (submitted && !codeOk ? "Enter the 6-digit code from the email." : "");
  const passwordError = submitted && !passwordOk ? "Use at least 12 characters." : "";

  const request = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitted(true);
    setError("");
    if (!emailOk) { focusFirstInvalid(cardRef.current); return; }
    setBusy(true);
    try {
      const r = await forgotPassword(email.trim(), formToken || undefined, turnstileToken || undefined);
      setChallenge({ id: r.challengeId, resendAfterSec: r.resendAfterSec });
      setStep("reset");
    } catch (err) {
      if (err instanceof SignupApiError && err.code === "email_quota_exceeded") {
        setError(mailSendError(err, "Couldn't send the code. Try again."));
      } else {
        setStep("reset"); // uniform 202 semantics: proceed to code entry regardless
      }
    } finally {
      setBusy(false);
    }
  };

  const reset = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitted(true);
    setError("");
    setCodeServerError("");
    if (!codeOk || !passwordOk) { focusFirstInvalid(cardRef.current); return; }
    setBusy(true);
    try {
      await resetPassword({ challengeId: challenge.id, code: code.trim(), newPassword });
      setStep("done");
      toast({ title: "Password updated", message: "Sign in with your new password.", tone: "success" });
    } catch (err) {
      if (err instanceof SignupApiError && err.code === "invalid_code") {
        setCodeServerError(codeEntryError(err, "That code isn't right. Try again."));
      } else {
        setError(codeEntryError(
          err instanceof SignupApiError ? err : { message: err instanceof Error ? err.message : "" },
          "Couldn't reset the password. Request a new code.",
        ));
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <AuthShell title="Reset your password" sub="We'll email a one-time code; your sessions stay signed out until the reset completes." onBack={onBack}>
      <div ref={cardRef}>
        {step === "email" && (
          <>
            <div className="lp-login-auth-head">
              <p className="lp-login-auth-kicker">Account recovery</p>
              <h2>Forgot password</h2>
              <p className="lp-login-auth-sub">Enter your work email — if an account exists we'll send a reset code.</p>
            </div>
            {error && (
              <div className="lp-login-alert lp-login-alert--warn" role="alert">
                <DtIcon name="alert" size={18} />
                <div><strong>Couldn't send the code</strong><p>{error}</p></div>
              </div>
            )}
            <form className="lp-login-form" onSubmit={request} noValidate>
              <div className={`lp-field ${emailError ? "is-error" : ""}`}>
                <label className="lp-label" htmlFor="forgot-email">Work email</label>
                <input
                  id="forgot-email" className="lp-input" type="email" inputMode="email"
                  value={email} onChange={(e) => setEmail(e.target.value)}
                  autoComplete="username" placeholder="you@company.com"
                  aria-invalid={Boolean(emailError)}
                  aria-describedby={emailError ? "forgot-email-error" : undefined}
                />
                {emailError && <small id="forgot-email-error" className="lp-field-error">{emailError}</small>}
              </div>
              <TurnstileWidget siteKey={turnstileSiteKey} onToken={setTurnstileToken} />
              <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block lp-login-submit" disabled={busy}>
                {busy ? "Sending…" : "Email me a reset code"}
                {!busy && <DtIcon name="arrow-right" size={16} />}
              </button>
            </form>
            <p className="lp-login-legal">
              Remembered it? <button type="button" onClick={onSignIn}>Back to sign in</button>
            </p>
          </>
        )}
        {step === "reset" && (
          <>
            <div className="lp-login-auth-head">
              <p className="lp-login-auth-kicker">Check your inbox</p>
              <h2>Enter reset code</h2>
              <p className="lp-login-auth-sub">If <strong>{email.trim()}</strong> has an account, a 6-digit code is on its way.</p>
            </div>
            {error && (
              <div className="lp-login-alert lp-login-alert--warn" role="alert">
                <DtIcon name="alert" size={18} />
                <div><strong>Couldn't reset the password</strong><p>{error}</p></div>
              </div>
            )}
            <form className="lp-login-form" onSubmit={reset} noValidate>
              <div className={`lp-field ${codeError ? "is-error" : ""}`}>
                <label className="lp-label" htmlFor="reset-code">Reset code</label>
                <input
                  id="reset-code" className="lp-input" inputMode="numeric" autoComplete="one-time-code"
                  maxLength={6} value={code}
                  onChange={(e) => { setCode(e.target.value.replace(/\D/g, "").slice(0, 6)); setCodeServerError(""); }}
                  placeholder="000000"
                  aria-invalid={Boolean(codeError)}
                  aria-describedby={codeError ? "reset-code-error" : undefined}
                />
                {codeError && <small id="reset-code-error" className="lp-field-error">{codeError}</small>}
              </div>
              <div className={`lp-field ${passwordError ? "is-error" : ""}`}>
                <label className="lp-label" htmlFor="reset-password">New password</label>
                <div className="lp-input-password">
                  <input
                    id="reset-password" className="lp-input"
                    type={showPassword ? "text" : "password"}
                    value={newPassword} onChange={(e) => setNewPassword(e.target.value)}
                    autoComplete="new-password" placeholder="At least 12 characters"
                    aria-invalid={Boolean(passwordError)}
                    aria-describedby={passwordError ? "reset-password-error" : undefined}
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
                {passwordError && <small id="reset-password-error" className="lp-field-error">{passwordError}</small>}
              </div>
              <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block lp-login-submit" disabled={busy}>
                {busy ? "Updating…" : "Set new password"}
                {!busy && <DtIcon name="arrow-right" size={16} />}
              </button>
            </form>
            {challenge.id && <ResendRow challengeId={challenge.id} initialCooldown={challenge.resendAfterSec} />}
          </>
        )}
        {step === "done" && (
          <div className="lp-login-auth-head">
            <p className="lp-login-auth-kicker">All set</p>
            <h2>Password updated</h2>
            <p className="lp-login-auth-sub">Your old sessions were signed out. Sign in with the new password.</p>
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block lp-login-submit lp-login-cta-spaced" onClick={onSignIn}>
              Sign in
              <DtIcon name="arrow-right" size={16} />
            </button>
          </div>
        )}
      </div>
    </AuthShell>
  );
}

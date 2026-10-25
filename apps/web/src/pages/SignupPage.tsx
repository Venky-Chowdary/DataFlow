import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { BrandWordmark } from "../components/BrandWordmark";
import { DtIcon } from "../components/DtIcon";
import { useToast } from "../components/Toast";
import {
  fetchFormToken,
  forgotPassword,
  resendCode,
  resetPassword,
  routeSsoForEmail,
  SignupApiError,
  signupRequest,
  verifyEmail,
} from "../lib/signupApi";
import { writeSession } from "../lib/session";

function isValidEmail(value: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());
}

interface AuthFlowProps {
  onAuthenticated: (email: string) => void;
  onSignIn: () => void;
  onBack: () => void;
}

/** Shared two-column chrome: brand rail left, form card right. */
function AuthShell({ title, sub, children }: { title: string; sub: string; children: React.ReactNode }) {
  return (
    <main className="lp-login lp-login--gate">
      <section className="lp-login-brand" aria-label="Datawrap">
        <div className="lp-login-brand-bg" aria-hidden>
          <span className="lp-login-brand-mesh" />
          <span className="lp-login-brand-orb lp-login-brand-orb--a" />
          <span className="lp-login-brand-orb lp-login-brand-orb--b" />
          <span className="lp-login-brand-rail" />
        </div>
        <div className="lp-login-brand-inner">
          <header className="lp-login-brand-head">
            <div className="lp-login-brand-logo">
              <BrandWordmark markSize={32} title="" />
            </div>
          </header>
          <div className="lp-login-brand-body">
            <p className="lp-login-brand-kicker">Enterprise data platform</p>
            <h1 className="lp-login-brand-title">{title}</h1>
            <p className="lp-login-brand-lead">{sub}</p>
            <div className="lp-login-proof" aria-hidden>
              <div className="lp-login-proof-flow">
                <span>Map</span>
                <i />
                <span>Preflight</span>
                <i />
                <span>Prove</span>
              </div>
              <p>Every session inherits workspace RBAC. Agents never skip gates.</p>
            </div>
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

/** Six-digit code entry with resend cooldown. */
function CodeStep({
  email,
  challengeId,
  resendAfterSec,
  onSubmitCode,
  busy,
  error,
}: {
  email: string;
  challengeId: string;
  resendAfterSec: number;
  onSubmitCode: (code: string) => void;
  busy: boolean;
  error: string;
}) {
  const [code, setCode] = useState("");
  const [cooldown, setCooldown] = useState(resendAfterSec);
  const [resending, setResending] = useState(false);
  const { toast } = useToast();

  useEffect(() => {
    if (cooldown <= 0) return;
    const t = window.setTimeout(() => setCooldown((s) => s - 1), 1000);
    return () => window.clearTimeout(t);
  }, [cooldown]);

  const resend = async () => {
    setResending(true);
    try {
      const r = await resendCode(challengeId);
      setCooldown(r.resendAfterSec || 60);
      toast({ title: "New code sent", message: "Check your inbox for the latest 6-digit code.", tone: "info" });
    } catch (err) {
      const msg = err instanceof SignupApiError && err.code === "resend_limited"
        ? `Too many resends — wait ${Math.max(1, Math.round(err.retryAfterSec / 60))} min.`
        : "Couldn't resend the code. Try again shortly.";
      toast({ title: "Resend unavailable", message: msg, tone: "warning" });
      if (err instanceof SignupApiError && err.retryAfterSec) setCooldown(err.retryAfterSec);
    } finally {
      setResending(false);
    }
  };

  return (
    <>
      <div className="lp-login-auth-head">
        <p className="lp-login-auth-kicker">Check your inbox</p>
        <h2>Enter the 6-digit code</h2>
        <p className="lp-login-auth-sub">
          We sent a verification code to <strong>{email}</strong>. It expires in 10 minutes.
        </p>
      </div>
      {error && (
        <div className="lp-login-alert lp-login-alert--warn" role="alert">
          <DtIcon name="alert" size={18} />
          <div><strong>Verification</strong><p>{error}</p></div>
        </div>
      )}
      <form className="lp-login-form" onSubmit={(e) => { e.preventDefault(); if (code.trim().length >= 6) onSubmitCode(code.trim()); }} noValidate>
        <div className="lp-field">
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
          />
        </div>
        <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block lp-login-submit" disabled={busy || code.length < 6}>
          {busy ? "Verifying…" : "Verify email"}
          {!busy && <DtIcon name="arrow-right" size={16} />}
        </button>
        <p className="lp-login-legal">
          Didn't get it?{" "}
          <button type="button" onClick={resend} disabled={cooldown > 0 || resending}>
            {cooldown > 0 ? `Resend in ${cooldown}s` : resending ? "Sending…" : "Resend code"}
          </button>
        </p>
      </form>
    </>
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
  const [formToken, setFormToken] = useState("");
  const [challenge, setChallenge] = useState({ id: initialChallengeId, resendAfterSec: 60 });
  const [ssoHint, setSsoHint] = useState<{ sso: string; startUrl: string } | null>(null);
  const mountedAt = useRef(Date.now());

  useEffect(() => {
    fetchFormToken("signup").then((t) => setFormToken(t ?? ""));
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
    if (!ready) return;
    setBusy(true);
    try {
      const r = await signupRequest({
        email: email.trim(),
        name: name.trim(),
        password,
        formToken: formToken || undefined,
      });
      setChallenge({ id: r.challengeId, resendAfterSec: r.resendAfterSec });
      setStep("verify");
    } catch (err) {
      if (err instanceof SignupApiError && err.code === "email_quota_exceeded") {
        setError(`We couldn't send your code right now — try again in ${Math.max(1, Math.round(err.retryAfterSec / 60))} min.`);
      } else if (err instanceof SignupApiError && err.code === "signup_rate_limited") {
        setError(`Too many attempts from this network — try again in ${Math.max(1, Math.round(err.retryAfterSec / 60))} min.`);
      } else {
        setError(err instanceof Error ? err.message : "Signup failed. Try again.");
      }
      toast({ title: "Couldn't start signup", message: "Check the fields and try again.", tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const submitCode = async (code: string) => {
    setBusy(true);
    setError("");
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
      if (err instanceof SignupApiError && err.code === "otp_expired") {
        setError("That code expired — request a fresh one below.");
      } else if (err instanceof SignupApiError) {
        setError(err.message || "Invalid code.");
      } else {
        setError("Invalid code. Check the email and try again.");
      }
    } finally {
      setBusy(false);
    }
  };

  if (step === "verify") {
    return (
      <AuthShell title="Verify your email" sub="One step left — confirm the address owns this workspace.">
        <CodeStep
          email={email.trim()}
          challengeId={challenge.id}
          resendAfterSec={challenge.resendAfterSec}
          onSubmitCode={submitCode}
          busy={busy}
          error={error}
        />
      </AuthShell>
    );
  }

  return (
    <AuthShell title="Create your workspace" sub="Verified email, governed transfers, and checksum proof — in minutes.">
      <button type="button" className="lp-login-back" onClick={onBack} style={{ marginBottom: 8 }}>
        <DtIcon name="chevron-left" size={15} />
        <span>Back</span>
      </button>
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
          <div><strong>Signup</strong><p>{error}</p></div>
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
          />
          {nameError && <small className="lp-field-error">{nameError}</small>}
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
          />
          {emailError && <small className="lp-field-error">{emailError}</small>}
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
        </div>

        <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block lp-login-submit" disabled={busy}>
          {busy ? "Sending code…" : "Send verification code"}
          {!busy && <DtIcon name="arrow-right" size={16} />}
        </button>
      </form>

      <p className="lp-login-legal">
        Already have an account? <button type="button" onClick={onSignIn}>Sign in</button>
      </p>
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
  const [challenge, setChallenge] = useState({ id: "", resendAfterSec: 60 });
  const [code, setCode] = useState("");
  const [newPassword, setNewPassword] = useState("");

  useEffect(() => {
    fetchFormToken("reset").then((t) => setFormToken(t ?? ""));
  }, []);

  const request = async (e: FormEvent) => {
    e.preventDefault();
    setError("");
    if (!isValidEmail(email)) {
      setError("Enter a valid work email.");
      return;
    }
    setBusy(true);
    try {
      const r = await forgotPassword(email.trim(), formToken || undefined);
      setChallenge({ id: r.challengeId, resendAfterSec: r.resendAfterSec });
      setStep("reset");
    } catch (err) {
      if (err instanceof SignupApiError && err.code === "email_quota_exceeded") {
        setError(`We couldn't send the code right now — try again in ${Math.max(1, Math.round(err.retryAfterSec / 60))} min.`);
      } else {
        setStep("reset"); // uniform 202 semantics: proceed to code entry regardless
      }
    } finally {
      setBusy(false);
    }
  };

  const reset = async (e: FormEvent) => {
    e.preventDefault();
    setError("");
    if (code.trim().length < 6 || newPassword.length < 12) {
      setError("Enter the 6-digit code and a password of at least 12 characters.");
      return;
    }
    setBusy(true);
    try {
      await resetPassword({ challengeId: challenge.id, code: code.trim(), newPassword });
      setStep("done");
      toast({ title: "Password updated", message: "Sign in with your new password.", tone: "success" });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Reset failed. Request a new code.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <AuthShell title="Reset your password" sub="We'll email a one-time code; your sessions stay signed out until the reset completes.">
      <button type="button" className="lp-login-back" onClick={onBack} style={{ marginBottom: 8 }}>
        <DtIcon name="chevron-left" size={15} />
        <span>Back</span>
      </button>
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
              <div><strong>Recovery</strong><p>{error}</p></div>
            </div>
          )}
          <form className="lp-login-form" onSubmit={request} noValidate>
            <div className="lp-field">
              <label className="lp-label" htmlFor="forgot-email">Work email</label>
              <input
                id="forgot-email" className="lp-input" type="email" inputMode="email"
                value={email} onChange={(e) => setEmail(e.target.value)}
                autoComplete="username" placeholder="you@company.com"
              />
            </div>
            <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block" disabled={busy}>
              {busy ? "Sending…" : "Email me a reset code"}
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
              <div><strong>Reset</strong><p>{error}</p></div>
            </div>
          )}
          <form className="lp-login-form" onSubmit={reset} noValidate>
            <div className="lp-field">
              <label className="lp-label" htmlFor="reset-code">Reset code</label>
              <input
                id="reset-code" className="lp-input" inputMode="numeric" autoComplete="one-time-code"
                maxLength={6} value={code}
                onChange={(e) => setCode(e.target.value.replace(/\D/g, "").slice(0, 6))}
                placeholder="000000"
              />
            </div>
            <div className="lp-field">
              <label className="lp-label" htmlFor="reset-password">New password</label>
              <input
                id="reset-password" className="lp-input" type="password"
                value={newPassword} onChange={(e) => setNewPassword(e.target.value)}
                autoComplete="new-password" placeholder="At least 12 characters"
              />
            </div>
            <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block" disabled={busy}>
              {busy ? "Updating…" : "Set new password"}
            </button>
          </form>
        </>
      )}
      {step === "done" && (
        <div className="lp-login-auth-head">
          <p className="lp-login-auth-kicker">All set</p>
          <h2>Password updated</h2>
          <p className="lp-login-auth-sub">Your old sessions were signed out. Sign in with the new password.</p>
          <button type="button" className="lp-btn lp-btn--brand lp-btn--lg lp-btn--block" onClick={onSignIn} style={{ marginTop: 16 }}>
            Sign in
          </button>
        </div>
      )}
    </AuthShell>
  );
}

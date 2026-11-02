import { FormEvent, ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { AuthBrandPanel } from "../components/AuthBrandPanel";
import { DtIcon } from "../components/DtIcon";
import { useToast } from "../components/Toast";
import {
  fetchFormToken,
  fetchSignupConfig,
  forgotPassword,
  inspectResetToken,
  resendCode,
  resetPassword,
  routeSsoForEmail,
  SignupApiError,
  signupRequest,
  verifiedSignupSession,
  verifyEmail,
} from "../lib/signupApi";
import { changeEmailRequestError, codeEntryError, mailSendError } from "../lib/authFlowCopy";
import { focusFirstInvalid, PasswordToggle, ResendRow } from "../components/AuthFlow";
import { AuthButton } from "../components/AuthButton";
import { TurnstileWidget } from "../components/TurnstileWidget";
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

export { turnstileRenderOptions } from "../components/TurnstileWidget";

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
        <AuthButton type="submit" size="lg" block className="lp-login-submit"
          disabled={code.length < 6} loading={busy} loadingLabel="Verifying…"
          trailingIcon={<DtIcon name="arrow-right" size={16} />}>
          Verify email
        </AuthButton>
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
      writeSession(verifiedSignupSession(result), true);
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
              <PasswordToggle show={showPassword} onToggle={() => setShowPassword((v) => !v)} />
            </div>
            {passwordError && <small id="signup-password-error" className="lp-field-error">{passwordError}</small>}
          </div>

          <TurnstileWidget siteKey={turnstileSiteKey} onToken={setTurnstileToken} action="signup" />

          <AuthButton type="submit" size="lg" block className="lp-login-submit"
            loading={busy} loadingLabel="Sending code…"
            trailingIcon={<DtIcon name="arrow-right" size={16} />}>
            Send verification code
          </AuthButton>
        </form>

        <p className="lp-login-legal">
          Already have an account? <AuthButton variant="link" onClick={onSignIn}>Sign in</AuthButton>
        </p>
      </div>
    </AuthShell>
  );
}

export function ForgotPasswordPage({ onSignIn, onBack }: { onSignIn: () => void; onBack: () => void }) {
  const { toast } = useToast();
  const [step, setStep] = useState<"email" | "sent">("email");
  const [email, setEmail] = useState("");
  const [busy, setBusy] = useState(false);
  const [resending, setResending] = useState(false);
  const [error, setError] = useState("");
  const [formToken, setFormToken] = useState("");
  const [turnstileSiteKey, setTurnstileSiteKey] = useState("");
  const [turnstileToken, setTurnstileToken] = useState("");
  const [cooldown, setCooldown] = useState(0);
  const [notice, setNotice] = useState("");
  const [submitted, setSubmitted] = useState(false);
  const cardRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    fetchFormToken("reset").then((t) => setFormToken(t ?? ""));
    fetchSignupConfig().then((c) => setTurnstileSiteKey(c.turnstileSiteKey));
  }, []);

  useEffect(() => {
    if (cooldown <= 0) return;
    const t = window.setTimeout(() => setCooldown((s) => s - 1), 1000);
    return () => window.clearTimeout(t);
  }, [cooldown]);

  const emailOk = isValidEmail(email);
  const emailError = submitted && !emailOk ? "Enter a valid work email." : "";

  const request = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitted(true);
    setError("");
    if (!emailOk) { focusFirstInvalid(cardRef.current); return; }
    setBusy(true);
    try {
      const r = await forgotPassword(email.trim(), formToken || undefined, turnstileToken || undefined);
      setCooldown(r.resendAfterSec || 60);
      setStep("sent");
      setNotice("");
    } catch (err) {
      if (err instanceof SignupApiError && err.code === "email_quota_exceeded") {
        setError(mailSendError(err, "Couldn't send the link. Try again."));
      } else {
        setStep("sent"); // uniform 202 semantics: never reveal existence
        setCooldown(60);
      }
    } finally {
      setBusy(false);
    }
  };

  const resend = async () => {
    setResending(true);
    setError("");
    try {
      const r = await forgotPassword(email.trim(), formToken || undefined, turnstileToken || undefined);
      setCooldown(r.resendAfterSec || 60);
      setNotice("New link sent — the earlier one stopped working.");
      toast({ title: "New link sent", message: "The earlier reset link was invalidated.", tone: "info" });
    } catch (err) {
      setNotice(mailSendError(
        err instanceof SignupApiError ? err : {},
        "Couldn't resend the link. Try again shortly.",
      ));
    } finally {
      setResending(false);
    }
  };

  return (
    <AuthShell title="Reset your password" sub="We'll email a secure one-time link; your sessions stay signed out until the reset completes." onBack={onBack}>
      <div ref={cardRef}>
        {step === "email" && (
          <>
            <div className="lp-login-auth-head">
              <p className="lp-login-auth-kicker">Account recovery</p>
              <h2>Forgot password</h2>
              <p className="lp-login-auth-sub">Enter your work email — if an account exists we'll send a reset link.</p>
            </div>
            {error && (
              <div className="lp-login-alert lp-login-alert--warn" role="alert">
                <DtIcon name="alert" size={18} />
                <div><strong>Couldn't send the link</strong><p>{error}</p></div>
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
              <AuthButton type="submit" size="lg" block className="lp-login-submit"
                loading={busy} loadingLabel="Sending…"
                trailingIcon={<DtIcon name="arrow-right" size={16} />}>
                Email me a reset link
              </AuthButton>
            </form>
            <p className="lp-login-legal">
              Remembered it? <AuthButton variant="link" onClick={onSignIn}>Back to sign in</AuthButton>
            </p>
          </>
        )}
        {step === "sent" && (
          <>
            <div className="lp-login-auth-head">
              <p className="lp-login-auth-kicker">Check your inbox</p>
              <h2>Reset link sent</h2>
              <p className="lp-login-auth-sub">
                If <strong>{email.trim()}</strong> has an account, a reset link is on its way.
                It expires in 30 minutes and works once.
              </p>
            </div>
            {error && (
              <div className="lp-login-alert lp-login-alert--warn" role="alert">
                <DtIcon name="alert" size={18} />
                <div><strong>Couldn't send the link</strong><p>{error}</p></div>
              </div>
            )}
            <div className="lp-login-resend">
              <p className="lp-login-legal">
                Didn't get it?{" "}
                <AuthButton variant="link" onClick={resend} disabled={cooldown > 0} loading={resending} loadingLabel="Sending…">
                  {cooldown > 0 ? `Resend in ${cooldown}s` : "Send a new link"}
                </AuthButton>
              </p>
              <p className="lp-login-resend-status" aria-live="polite">
                {notice || (cooldown > 0 ? `You can request a new link in ${cooldown} seconds.` : "")}
              </p>
            </div>
            <p className="lp-login-legal">
              Remembered it? <AuthButton variant="link" onClick={onSignIn}>Back to sign in</AuthButton>
            </p>
          </>
        )}
      </div>
    </AuthShell>
  );
}


/** Landing page for the emailed reset link: validates the token, strips it
 *  from the URL, then collects the new password. */
export function ResetPasswordPage({ token, onSignIn, onSendNewLink }: {
  token: string;
  onSignIn: () => void;
  onSendNewLink: () => void;
}) {
  const { toast } = useToast();
  const [status, setStatus] = useState<"checking" | "ok" | "invalid" | "done">("checking");
  const [newPassword, setNewPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [submitted, setSubmitted] = useState(false);
  const cardRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    // The token must not survive in the URL (history, referers, screenshots).
    window.history.replaceState({}, "", "/#/reset-password");
    let meta = document.querySelector('meta[name="referrer"]');
    if (!meta) {
      meta = document.createElement("meta");
      meta.setAttribute("name", "referrer");
      document.head.appendChild(meta);
    }
    meta.setAttribute("content", "no-referrer");
    let live = true;
    inspectResetToken(token)
      .then((r) => { if (live) setStatus(r.ok ? "ok" : "invalid"); })
      .catch(() => { if (live) setStatus("invalid"); });
    return () => { live = false; };
  }, [token]);

  const passwordOk = newPassword.length >= 12;
  const passwordError = submitted && !passwordOk ? "Use at least 12 characters." : "";

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitted(true);
    setError("");
    if (!passwordOk) { focusFirstInvalid(cardRef.current); return; }
    setBusy(true);
    try {
      await resetPassword({ token, newPassword });
      setStatus("done");
      toast({ title: "Password updated", message: "Sign in with your new password.", tone: "success" });
    } catch (err) {
      if (err instanceof SignupApiError && err.code === "link_expired") {
        setStatus("invalid");
      } else {
        setError(codeEntryError(
          err instanceof SignupApiError ? err : { message: err instanceof Error ? err.message : "" },
          "Couldn't reset the password. Try the link again.",
        ));
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <AuthShell title="Reset your password" sub="Choose a new password for your Datawrap account." onBack={onSignIn}>
      <div ref={cardRef}>
        {status === "checking" && (
          <div className="lp-login-auth-head">
            <p className="lp-login-auth-kicker">Account recovery</p>
            <h2>Checking your link…</h2>
          </div>
        )}
        {status === "invalid" && (
          <div className="lp-login-auth-head">
            <p className="lp-login-auth-kicker">Account recovery</p>
            <h2>This link has expired or was already used</h2>
            <p className="lp-login-auth-sub">Reset links work once and expire after 30 minutes. Request a fresh one.</p>
            <AuthButton size="lg" block className="lp-login-submit lp-login-cta-spaced" onClick={onSendNewLink}
              trailingIcon={<DtIcon name="arrow-right" size={16} />}>
              Send a new link
            </AuthButton>
          </div>
        )}
        {status === "ok" && (
          <>
            <div className="lp-login-auth-head">
              <p className="lp-login-auth-kicker">Account recovery</p>
              <h2>Choose a new password</h2>
              <p className="lp-login-auth-sub">After this change every device is signed out.</p>
            </div>
            {error && (
              <div className="lp-login-alert lp-login-alert--warn" role="alert">
                <DtIcon name="alert" size={18} />
                <div><strong>Couldn't reset the password</strong><p>{error}</p></div>
              </div>
            )}
            <form className="lp-login-form" onSubmit={submit} noValidate>
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
                  <PasswordToggle show={showPassword} onToggle={() => setShowPassword((v) => !v)} />
                </div>
                {passwordError && <small id="reset-password-error" className="lp-field-error">{passwordError}</small>}
              </div>
              <AuthButton type="submit" size="lg" block className="lp-login-submit"
                loading={busy} loadingLabel="Updating…"
                trailingIcon={<DtIcon name="arrow-right" size={16} />}>
                Set new password
              </AuthButton>
            </form>
          </>
        )}
        {status === "done" && (
          <div className="lp-login-auth-head">
            <p className="lp-login-auth-kicker">All set</p>
            <h2>Password updated</h2>
            <p className="lp-login-auth-sub">Your old sessions were signed out. Sign in with the new password.</p>
            <AuthButton size="lg" block className="lp-login-submit lp-login-cta-spaced" onClick={onSignIn}
              trailingIcon={<DtIcon name="arrow-right" size={16} />}>
              Sign in
            </AuthButton>
          </div>
        )}
      </div>
    </AuthShell>
  );
}

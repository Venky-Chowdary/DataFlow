import { FormEvent, useEffect, useState } from "react";
import { Button } from "./ui/Button";
import {
  confirmEmailChange,
  fetchSignupConfig,
  requestEmailChange,
  SignupApiError,
} from "../lib/signupApi";
import { changeEmailConfirmError, changeEmailRequestError } from "../lib/authFlowCopy";
import { useToast } from "./Toast";

function isValidEmail(value: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());
}

/**
 * Settings → Security: change the sign-in email. Step 1 sends a 6-digit
 * code to the NEW address (the account stays unchanged until confirmed);
 * step 2 verifies it. Only renders when the deployment runs the
 * self-serve signup surface (the email-change endpoints live there).
 */
export function ChangeEmailCard({ enabled: enabledProp, initialStep }: {
  enabled?: boolean | null;
  /** Test/a11y hook: render a later step in static markup. */
  initialStep?: "idle" | "request" | "code" | "done";
} = {}) {
  const { toast } = useToast();
  const [enabled, setEnabled] = useState<boolean | null>(enabledProp ?? null);
  const [step, setStep] = useState<"idle" | "request" | "code" | "done">(initialStep ?? "idle");
  const [newEmail, setNewEmail] = useState("");
  const [code, setCode] = useState("");
  const [challengeId, setChallengeId] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [submitted, setSubmitted] = useState(false);

  useEffect(() => {
    if (enabledProp === undefined) {
      fetchSignupConfig().then((c) => setEnabled(c.enabled));
    } else {
      setEnabled(enabledProp);
    }
  }, [enabledProp]);

  if (!enabled) return null;

  const emailError = submitted && !isValidEmail(newEmail) ? "Enter a valid email." : "";
  const codeError = submitted && code.trim().length !== 6 ? "Enter the 6-digit code." : "";

  const request = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitted(true);
    setError("");
    if (!isValidEmail(newEmail)) return;
    setBusy(true);
    try {
      const r = await requestEmailChange(newEmail.trim());
      setChallengeId(r.challengeId);
      setSubmitted(false);
      setStep("code");
    } catch (err) {
      setError(changeEmailRequestError(
        err instanceof SignupApiError ? err : { message: err instanceof Error ? err.message : "" },
      ));
    } finally {
      setBusy(false);
    }
  };

  const confirm = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitted(true);
    setError("");
    if (code.trim().length !== 6) return;
    setBusy(true);
    try {
      await confirmEmailChange(challengeId, code.trim());
      setStep("done");
      toast({
        title: "Sign-in email changed",
        message: "Your other sessions were signed out. Sign back in with the new address.",
        tone: "success",
      });
    } catch (err) {
      setError(changeEmailConfirmError(
        err instanceof SignupApiError ? err : { message: err instanceof Error ? err.message : "" },
      ));
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <h3 className="df2-settings-subhead df2-settings-subhead--spaced">Sign-in email</h3>
      <div className="df2-settings-policy-grid" data-testid="change-email-card">
        {step === "idle" && (
          <div className="df2-settings-policy-row">
            <div>
              <h3>Change sign-in email</h3>
              <p>We email a 6-digit code to the new address — the change only lands after you confirm it.</p>
            </div>
            <Button variant="primary" size="sm" onClick={() => setStep("request")}>
              Change email
            </Button>
          </div>
        )}

        {step === "request" && (
          <form className="df2-settings-policy-row" onSubmit={request} noValidate>
            <div>
              <h3 id="change-email-input-label">New sign-in email</h3>
              <p>A 6-digit code goes to this address. Your sessions keep working until you confirm.</p>
              <input
                id="change-email-input"
                aria-labelledby="change-email-input-label"
                className="df2-input"
                type="email"
                inputMode="email"
                autoComplete="username"
                placeholder="you@company.com"
                value={newEmail}
                onChange={(e) => { setNewEmail(e.target.value); setError(""); }}
                aria-invalid={Boolean(emailError || error)}
                aria-describedby={emailError || error ? "change-email-error" : undefined}
              />
              {(emailError || error) && (
                <p id="change-email-error" className="df2-label-hint df2-field-error-text">{emailError || error}</p>
              )}
            </div>
            <span className="df2-change-email-actions">
              <Button variant="secondary" size="sm" onClick={() => { setStep("idle"); setError(""); setSubmitted(false); }}>
                Cancel
              </Button>
              <Button variant="primary" size="sm" type="submit" loading={busy} loadingLabel="Sending…">
                Send code
              </Button>
            </span>
          </form>
        )}

        {step === "code" && (
          <form className="df2-settings-policy-row" onSubmit={confirm} noValidate>
            <div>
              <h3 id="change-email-code-label">Enter the 6-digit code</h3>
              <p>We sent it to <strong>{newEmail.trim()}</strong> — it expires in 10 minutes.</p>
              <input
                id="change-email-code"
                aria-labelledby="change-email-code-label"
                className="df2-input"
                inputMode="numeric"
                autoComplete="one-time-code"
                maxLength={6}
                placeholder="000000"
                value={code}
                onChange={(e) => { setCode(e.target.value.replace(/\D/g, "").slice(0, 6)); setError(""); }}
                aria-invalid={Boolean(codeError || error)}
                aria-describedby={codeError || error ? "change-email-code-error" : undefined}
              />
              {(codeError || error) && (
                <p id="change-email-code-error" className="df2-label-hint df2-field-error-text">{codeError || error}</p>
              )}
            </div>
            <span className="df2-change-email-actions">
              <Button variant="secondary" size="sm" onClick={() => { setStep("request"); setError(""); setSubmitted(false); setCode(""); }}>
                Back
              </Button>
              <Button variant="primary" size="sm" type="submit" disabled={code.length < 6} loading={busy} loadingLabel="Confirming…">
                Confirm change
              </Button>
            </span>
          </form>
        )}

        {step === "done" && (
          <div className="df2-settings-policy-row">
            <div>
              <h3>Email changed</h3>
              <p>Your sign-in email is now <strong>{newEmail.trim()}</strong>. Other sessions were signed out — sign back in with the new address.</p>
            </div>
            <span className="df2-badge df2-badge-live">Done</span>
          </div>
        )}
      </div>
    </>
  );
}

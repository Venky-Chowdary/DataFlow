import { useState, type FormEvent, type ReactNode } from "react";

import type { LoginSession } from "../lib/api";
import {
  completeMfaLogin,
  isCompleteTotp,
  isRecoveryCodeShape,
  mfaErrorMessage,
  normalizeTotpInput,
  verifyMfaSession,
} from "../lib/mfaApi";
import { usePermissions } from "../lib/PermissionsContext";
import { AuthButton } from "./AuthButton";
import { DtIcon } from "./DtIcon";
import { TwoFactorSetup } from "./TwoFactorSetup";

type Proof = { code?: string; recoveryCode?: string };

/**
 * One code field: a 6-digit authenticator code, or (switchable) a recovery code.
 * The API decides whether it is right; this only shapes the input.
 */
export function MfaCodeForm({
  idPrefix,
  submitLabel,
  onSubmit,
  footer,
}: {
  idPrefix: string;
  submitLabel: string;
  onSubmit: (proof: Proof) => Promise<void>;
  footer?: ReactNode;
}) {
  const [useRecovery, setUseRecovery] = useState(false);
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (useRecovery ? !isRecoveryCodeShape(value) : !isCompleteTotp(value)) {
      setError(useRecovery
        ? "Recovery codes have 16 letters and digits, like abcd-efgh-jkmn-pqrs."
        : "Enter the 6-digit code from your authenticator app.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      await onSubmit(useRecovery ? { recoveryCode: value } : { code: value });
    } catch (err) {
      setError(mfaErrorMessage(err));
      setValue("");
    } finally {
      setBusy(false);
    }
  };

  const fieldId = `${idPrefix}-${useRecovery ? "recovery" : "code"}`;
  return (
    <form className="lp-login-form" onSubmit={submit} noValidate>
      {error && (
        <div className="lp-login-alert lp-login-alert--warn" role="alert">
          <DtIcon name="alert" size={18} />
          <div>
            <strong>Code not accepted</strong>
            <p id={`${idPrefix}-error`}>{error}</p>
          </div>
        </div>
      )}
      <div className={`lp-field ${error ? "is-error" : ""}`}>
        <label className="lp-label" htmlFor={fieldId}>
          {useRecovery ? "Recovery code" : "Authentication code"}
        </label>
        <input
          id={fieldId}
          className="lp-input"
          inputMode={useRecovery ? "text" : "numeric"}
          autoComplete="one-time-code"
          autoCapitalize="none"
          spellCheck={false}
          autoFocus
          placeholder={useRecovery ? "abcd-efgh-jkmn-pqrs" : "123456"}
          value={value}
          onChange={(e) => { setValue(useRecovery ? e.target.value : normalizeTotpInput(e.target.value)); setError(""); }}
          aria-invalid={Boolean(error)}
          aria-describedby={error ? `${idPrefix}-error` : undefined}
        />
      </div>
      <AuthButton type="submit" size="lg" block loading={busy} loadingLabel="Checking…">
        {submitLabel}
      </AuthButton>
      <p className="lp-login-legal">
        <AuthButton
          variant="link"
          onClick={() => { setUseRecovery((v) => !v); setValue(""); setError(""); }}
        >
          {useRecovery ? "Use a code from my authenticator app" : "Lost your phone? Use a recovery code"}
        </AuthButton>
      </p>
      {footer}
    </form>
  );
}

/** Sign-in, step two: the password matched; now the code. */
export function LoginMfaStep({
  challenge,
  email,
  onSignedIn,
  onBack,
}: {
  challenge: string;
  email: string;
  onSignedIn: (session: LoginSession & { recovery_codes_remaining?: number; mfa_method?: string }) => void;
  onBack: () => void;
}) {
  return (
    <div data-testid="mfa-login-step">
      <div className="lp-login-auth-head">
        <p className="lp-login-auth-kicker">Two-factor authentication</p>
        <h2 id="login-form-title">Enter your code</h2>
        <p className="lp-login-auth-sub">
          Open your authenticator app and enter the 6-digit code for <strong>{email}</strong>.
        </p>
      </div>
      <MfaCodeForm
        idPrefix="mfa-login"
        submitLabel="Verify and sign in"
        onSubmit={async (proof) => onSignedIn(await completeMfaLogin(challenge, proof))}
        footer={
          <p className="lp-login-legal">
            <AuthButton variant="link" onClick={onBack}>Back to sign in</AuthButton>
          </p>
        }
      />
    </div>
  );
}

/**
 * The workspace "require two-factor" hold. When the API says this session is
 * held, nothing else would answer anyway — show the one thing that will:
 * set up an authenticator app (or, if already set up, enter a code).
 */
export function MfaSessionGate({ children, onSignOut }: { children: ReactNode; onSignOut?: () => void }) {
  const { identity, refresh } = usePermissions();
  const hold = identity?.mfa?.hold ?? null;
  if (!hold) return <>{children}</>;

  const enrolling = hold === "mfa_enrollment_required";
  return (
    <main className="lp-login lp-login--gate" data-testid="mfa-session-gate">
      <section className="lp-login-auth" aria-labelledby="mfa-gate-title">
        <div className="lp-login-auth-inner">
          <div className="lp-login-auth-card">
            <div className="lp-login-auth-head">
              <p className="lp-login-auth-kicker">Required by your workspace</p>
              <h2 id="mfa-gate-title">
                {enrolling ? "Set up two-factor authentication" : "Confirm it's you"}
              </h2>
              <p className="lp-login-auth-sub">
                {enrolling
                  ? <>Your workspace requires a code from an authenticator app at every sign-in. Set it up for <strong>{identity?.email}</strong> to continue — the rest of Datawrap opens as soon as it is on.</>
                  : <>Your workspace requires two-factor authentication. Enter a code from your authenticator app to continue.</>}
              </p>
            </div>
            {enrolling ? (
              <div className="df2-settings-policy-grid">
                <TwoFactorSetup idPrefix="mfa-gate-setup" onDone={() => void refresh()} />
              </div>
            ) : (
              <MfaCodeForm
                idPrefix="mfa-gate"
                submitLabel="Continue"
                onSubmit={async (proof) => { await verifyMfaSession(proof); await refresh(); }}
              />
            )}
            {onSignOut && (
              <p className="lp-login-legal">
                <AuthButton variant="link" onClick={onSignOut}>Sign out</AuthButton>
              </p>
            )}
          </div>
        </div>
      </section>
    </main>
  );
}

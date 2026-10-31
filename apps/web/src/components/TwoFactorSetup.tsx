import { useState, type FormEvent } from "react";

import {
  confirmMfaEnrollment,
  formatSetupKey,
  isCompleteTotp,
  mfaErrorMessage,
  normalizeTotpInput,
  recoveryCodesFile,
  startMfaEnrollment,
  type MfaEnrollment,
  type MfaReauth,
} from "../lib/mfaApi";
import { Button } from "./ui/Button";

/**
 * Set up an authenticator app: get a setup key, prove it with a code, save
 * the recovery codes. Shared by Settings → Security and the sign-in hold.
 *
 * No QR library ships with the web app, so the key is shown as text and as an
 * ``otpauth://`` link (which phone authenticator apps open directly).
 */
export function TwoFactorSetup({
  proof,
  onDone,
  onCancel,
  idPrefix = "mfa-setup",
  initial,
}: {
  /** Re-enrolment needs a fresh code or the password; first setup does not. */
  proof?: MfaReauth;
  onDone: () => void;
  onCancel?: () => void;
  idPrefix?: string;
  /** Test hook: render a later step in static markup. */
  initial?: { enrollment?: MfaEnrollment; codes?: string[] };
}) {
  const [enrollment, setEnrollment] = useState<MfaEnrollment | null>(initial?.enrollment ?? null);
  const [codes, setCodes] = useState<string[] | null>(initial?.codes ?? null);
  const [code, setCode] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const begin = async () => {
    setBusy(true);
    setError("");
    try {
      setEnrollment(await startMfaEnrollment(proof));
    } catch (err) {
      setError(mfaErrorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  const confirm = async (event: FormEvent) => {
    event.preventDefault();
    if (!isCompleteTotp(code)) {
      setError("Enter the 6-digit code your authenticator app shows.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      setCodes(await confirmMfaEnrollment(code));
    } catch (err) {
      setError(mfaErrorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  const errorLine = error ? (
    <p id={`${idPrefix}-error`} className="df2-label-hint df2-field-error-text" role="alert">{error}</p>
  ) : null;

  if (codes) {
    return <RecoveryCodesPanel codes={codes} account={enrollment?.account ?? ""} idPrefix={idPrefix} onDone={onDone} />;
  }

  if (!enrollment) {
    return (
      <div className="df2-settings-policy-row">
        <div>
          <h3>Set up an authenticator app</h3>
          <p>
            Use Google Authenticator, Microsoft Authenticator, 1Password, Authy or any app that supports
            time-based codes (TOTP). You will need your phone for this step.
          </p>
          {errorLine}
        </div>
        <span className="df2-change-email-actions">
          {onCancel && <Button variant="secondary" size="sm" onClick={onCancel}>Cancel</Button>}
          <Button variant="primary" size="sm" loading={busy} loadingLabel="Starting…" onClick={() => void begin()}>
            Get setup key
          </Button>
        </span>
      </div>
    );
  }

  return (
    <form className="df2-settings-policy-row" onSubmit={confirm} noValidate data-testid="mfa-setup-confirm">
      <div>
        <h3>Add Datawrap to your authenticator app</h3>
        <p>
          On your phone, <a href={enrollment.otpauth_uri}>open this setup link</a> or choose “Enter a setup key” in
          your app and type the key below (account: <strong>{enrollment.account}</strong>, time-based).
        </p>
        <p className="df2-mono" aria-label="Setup key">{formatSetupKey(enrollment.secret)}</p>
        <label className="df2-label" htmlFor={`${idPrefix}-code`}>6-digit code from the app</label>
        <input
          id={`${idPrefix}-code`}
          className="df2-input"
          inputMode="numeric"
          autoComplete="one-time-code"
          maxLength={7}
          placeholder="123456"
          value={code}
          onChange={(e) => { setCode(normalizeTotpInput(e.target.value)); setError(""); }}
          aria-invalid={Boolean(error)}
          aria-describedby={error ? `${idPrefix}-error` : undefined}
        />
        {errorLine}
      </div>
      <span className="df2-change-email-actions">
        {onCancel && <Button variant="secondary" size="sm" onClick={onCancel}>Cancel</Button>}
        <Button variant="primary" size="sm" type="submit" loading={busy} loadingLabel="Checking…">
          Turn on two-factor
        </Button>
      </span>
    </form>
  );
}

/** Recovery codes, shown once: download them, tick "saved", then continue. */
export function RecoveryCodesPanel({
  codes,
  account,
  idPrefix,
  onDone,
  intro = "Two-factor authentication is on. If you lose your phone, each of these codes signs you in once.",
}: {
  codes: string[];
  account: string;
  idPrefix: string;
  onDone: () => void;
  intro?: string;
}) {
  const [saved, setSaved] = useState(false);

  const download = () => {
    const blob = new Blob([recoveryCodesFile(codes, account || "your account")], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "datawrap-recovery-codes.txt";
    link.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="df2-settings-policy-row" data-testid="mfa-recovery-codes">
      <div>
        <h3>Save your recovery codes</h3>
        <p>
          {intro} They are shown only now — download or copy them and keep them somewhere safe.
        </p>
        <ol className="df2-mono" aria-label="Recovery codes">
          {codes.map((c) => <li key={c}>{c}</li>)}
        </ol>
        <label className="df2-settings-hint" htmlFor={`${idPrefix}-saved`}>
          <input id={`${idPrefix}-saved`} type="checkbox" checked={saved} onChange={(e) => setSaved(e.target.checked)} />{" "}
          I saved these recovery codes
        </label>
      </div>
      <span className="df2-change-email-actions">
        <Button variant="secondary" size="sm" onClick={download}>Download</Button>
        <Button variant="primary" size="sm" disabled={!saved} onClick={onDone}>Done</Button>
      </span>
    </div>
  );
}

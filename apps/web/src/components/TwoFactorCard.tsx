import { useCallback, useEffect, useState, type FormEvent } from "react";

import {
  disableMfa,
  fetchMfaStatus,
  mfaErrorMessage,
  normalizeTotpInput,
  regenerateRecoveryCodes,
  type MfaReauth,
  type MfaStatus,
} from "../lib/mfaApi";
import { usePermissions } from "../lib/PermissionsContext";
import { RecoveryCodesPanel, TwoFactorSetup } from "./TwoFactorSetup";
import { useToast } from "./Toast";
import { Button } from "./ui/Button";

type Mode = "view" | "setup" | "reenroll" | "codes" | "disable" | "newcodes";

/**
 * Settings → Security → Two-factor authentication, for the signed-in person.
 *
 * Re-enrolment, new recovery codes and turning it off each ask for a current
 * code (or a recovery code) or the account password first — the API checks it.
 */
export function TwoFactorCard({ initialStatus }: { initialStatus?: MfaStatus | null } = {}) {
  const { toast } = useToast();
  const { refresh, identity } = usePermissions();
  const [status, setStatus] = useState<MfaStatus | null>(initialStatus ?? null);
  const [loadError, setLoadError] = useState("");
  const [mode, setMode] = useState<Mode>("view");
  const [proof, setProof] = useState<MfaReauth | undefined>(undefined);
  const [newCodes, setNewCodes] = useState<string[] | null>(null);

  const load = useCallback(async () => {
    try {
      setStatus(await fetchMfaStatus());
      setLoadError("");
    } catch (err) {
      setLoadError(mfaErrorMessage(err));
    }
  }, []);

  useEffect(() => {
    if (initialStatus === undefined) void load();
  }, [initialStatus, load]);

  const finished = async (message: string) => {
    setMode("view");
    setProof(undefined);
    setNewCodes(null);
    toast({ title: "Two-factor authentication", message, tone: "success" });
    await load();
    await refresh();
  };

  return (
    <>
      <h3 className="df2-settings-subhead df2-settings-subhead--spaced">Two-factor authentication</h3>
      <div className="df2-settings-policy-grid" data-testid="two-factor-card">
        {loadError && (
          <div className="df2-settings-policy-row">
            <div>
              <h3>Two-factor settings unavailable</h3>
              <p>{loadError}</p>
            </div>
          </div>
        )}

        {status && mode === "view" && (
          <div className="df2-settings-policy-row">
            <div>
              <h3>Authenticator app (TOTP)</h3>
              <p>{statusSentence(status)}</p>
            </div>
            <span className="df2-change-email-actions">
              {status.enrolled ? (
                <>
                  <Button variant="secondary" size="sm" onClick={() => setMode("newcodes")}>New recovery codes</Button>
                  <Button variant="secondary" size="sm" onClick={() => setMode("reenroll")}>Move to a new phone</Button>
                  {!status.required && (
                    <Button variant="danger" size="sm" onClick={() => setMode("disable")}>Turn off</Button>
                  )}
                </>
              ) : (
                <Button variant="primary" size="sm" onClick={() => setMode("setup")}>Set up</Button>
              )}
            </span>
          </div>
        )}

        {mode === "setup" && (
          <TwoFactorSetup onDone={() => void finished("Two-factor authentication is on.")} onCancel={() => setMode("view")} />
        )}

        {mode === "reenroll" && !proof && (
          <ReauthForm
            idPrefix="mfa-reenroll"
            title="Confirm it's you"
            hint="Enter a current code from your authenticator app, a recovery code, or your password. Your old app keeps working until the new one is confirmed."
            action="Continue"
            onCancel={() => setMode("view")}
            onSubmit={async (p) => setProof(p)}
          />
        )}
        {mode === "reenroll" && proof && (
          <TwoFactorSetup
            idPrefix="mfa-reenroll-setup"
            proof={proof}
            onDone={() => void finished("Your new authenticator app is set up.")}
            onCancel={() => { setMode("view"); setProof(undefined); }}
          />
        )}

        {mode === "newcodes" && !newCodes && (
          <ReauthForm
            idPrefix="mfa-newcodes"
            title="Create new recovery codes"
            hint="Your previous recovery codes stop working. Confirm with a current code or your password."
            action="Create codes"
            onCancel={() => setMode("view")}
            onSubmit={async (p) => setNewCodes(await regenerateRecoveryCodes(p))}
          />
        )}
        {mode === "newcodes" && newCodes && (
          <RecoveryCodesPanel
            codes={newCodes}
            account={identity?.email ?? ""}
            idPrefix="mfa-newcodes-list"
            intro="Your previous recovery codes no longer work."
            onDone={() => void finished("New recovery codes are active.")}
          />
        )}

        {mode === "disable" && (
          <ReauthForm
            idPrefix="mfa-disable"
            title="Turn off two-factor authentication"
            hint="Signing in will only need your password. Confirm with a current code or your password."
            action="Turn off"
            danger
            onCancel={() => setMode("view")}
            onSubmit={async (p) => { await disableMfa(p); await finished("Two-factor authentication is off."); }}
          />
        )}
      </div>
    </>
  );
}

function statusSentence(status: MfaStatus): string {
  if (status.via_sso && !status.enrolled) {
    return "You signed in with single sign-on; your identity provider handles two-factor authentication.";
  }
  if (!status.enrolled) {
    return status.required
      ? "Required by your workspace. Set up an authenticator app to keep using Datawrap."
      : "Off. Add a code from your phone to every password sign-in.";
  }
  const remaining = `${status.recovery_codes_remaining} of 10 recovery codes left`;
  const required = status.required ? " Required by your workspace, so it cannot be turned off." : "";
  return `On — sign-in asks for a code from your authenticator app. ${remaining}.${required}`;
}

/** Proof for a sensitive change: a code, a recovery code, or the password. */
export function ReauthForm({
  idPrefix,
  title,
  hint,
  action,
  danger,
  onSubmit,
  onCancel,
}: {
  idPrefix: string;
  title: string;
  hint: string;
  action: string;
  danger?: boolean;
  onSubmit: (proof: MfaReauth) => Promise<void>;
  onCancel: () => void;
}) {
  const [method, setMethod] = useState<"code" | "password">("code");
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!value.trim()) {
      setError(method === "code" ? "Enter a code." : "Enter your password.");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const digits = normalizeTotpInput(value);
      const proof: MfaReauth =
        method === "password" ? { password: value } : digits.length === 6 && /^\d[\d\s]*$/.test(value.trim()) ? { code: digits } : { recoveryCode: value };
      await onSubmit(proof);
    } catch (err) {
      setError(mfaErrorMessage(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <form className="df2-settings-policy-row" onSubmit={submit} noValidate>
      <div>
        <h3>{title}</h3>
        <p>{hint}</p>
        <label className="df2-label" htmlFor={`${idPrefix}-value`}>
          {method === "code" ? "Authenticator or recovery code" : "Password"}
        </label>
        <input
          id={`${idPrefix}-value`}
          className="df2-input"
          type={method === "password" ? "password" : "text"}
          autoComplete={method === "password" ? "current-password" : "one-time-code"}
          value={value}
          onChange={(e) => { setValue(e.target.value); setError(""); }}
          aria-invalid={Boolean(error)}
          aria-describedby={error ? `${idPrefix}-error` : undefined}
        />
        {error && <p id={`${idPrefix}-error`} className="df2-label-hint df2-field-error-text" role="alert">{error}</p>}
        <Button variant="ghost" size="sm" onClick={() => { setMethod(method === "code" ? "password" : "code"); setValue(""); setError(""); }}>
          {method === "code" ? "Use my password instead" : "Use a code instead"}
        </Button>
      </div>
      <span className="df2-change-email-actions">
        <Button variant="secondary" size="sm" onClick={onCancel}>Cancel</Button>
        <Button variant={danger ? "danger" : "primary"} size="sm" type="submit" loading={busy} loadingLabel="Checking…">
          {action}
        </Button>
      </span>
    </form>
  );
}

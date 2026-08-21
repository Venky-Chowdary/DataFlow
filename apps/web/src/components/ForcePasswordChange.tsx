import { useState, type FormEvent, type ReactNode } from "react";

import { Button } from "@dataflow/design-system";

import { changeOwnPassword } from "../lib/api";
import { usePermissions } from "../lib/PermissionsContext";
import { DtIcon } from "./DtIcon";
import { useToast } from "./Toast";

/**
 * An admin-issued one-time password is retired before the workspace opens.
 *
 * The Team screen promises the operator "they will be asked to change it at
 * first sign-in". Nothing asked: the account went straight in and the temporary
 * password stayed valid indefinitely. This gate keeps that promise — the API
 * already marks the account (``must_change_password``) and rotates it through
 * ``POST /auth/change-password``.
 */
export function ForcePasswordChange({ children }: { children: ReactNode }) {
  const { identity, refresh } = usePermissions();
  const { toast } = useToast();
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [repeat, setRepeat] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  if (!identity?.must_change_password) return <>{children}</>;

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (next.length < 12) {
      setError("Choose a new password of at least 12 characters.");
      return;
    }
    if (next !== repeat) {
      setError("The two new passwords do not match.");
      return;
    }
    setSaving(true);
    setError("");
    try {
      await changeOwnPassword(current, next);
      toast({ title: "Password changed", message: "Your temporary password is retired.", tone: "success" });
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not change your password.");
    } finally {
      setSaving(false);
    }
  };

  return (
    <main className="lp-login lp-login--gate" data-testid="force-password-change">
      <section className="lp-login-auth" aria-labelledby="force-password-title">
        <div className="lp-login-auth-inner">
          <div className="lp-login-auth-card">
            <div className="lp-login-auth-head">
              <p className="lp-login-auth-kicker">One-time password</p>
              <h2 id="force-password-title">Change your temporary password</h2>
              <p className="lp-login-auth-sub">
                <strong>{identity.email}</strong> was created with a password an administrator issued. Set your
                own password to open the workspace.
              </p>
            </div>

            {error && (
              <div className="lp-login-alert lp-login-alert--danger" role="alert">
                <DtIcon name="alert" size={18} />
                <div>
                  <strong>Password not changed</strong>
                  <p>{error}</p>
                </div>
              </div>
            )}

            <form className="lp-login-form" onSubmit={submit} noValidate>
              <div className="lp-field">
                <label className="lp-label" htmlFor="fpc-current">Temporary password</label>
                <input
                  id="fpc-current"
                  className="lp-input"
                  type="password"
                  autoComplete="current-password"
                  value={current}
                  onChange={(e) => setCurrent(e.target.value)}
                />
              </div>

              <div className="lp-field">
                <label className="lp-label" htmlFor="fpc-next">New password</label>
                <input
                  id="fpc-next"
                  className="lp-input"
                  type="password"
                  autoComplete="new-password"
                  value={next}
                  onChange={(e) => setNext(e.target.value)}
                  placeholder="At least 12 characters"
                />
              </div>

              <div className="lp-field">
                <label className="lp-label" htmlFor="fpc-repeat">Repeat new password</label>
                <input
                  id="fpc-repeat"
                  className="lp-input"
                  type="password"
                  autoComplete="new-password"
                  value={repeat}
                  onChange={(e) => setRepeat(e.target.value)}
                />
              </div>

              <Button type="submit" variant="primary" disabled={saving}>
                {saving ? "Changing…" : "Change password and continue"}
              </Button>
            </form>
          </div>
        </div>
      </section>
    </main>
  );
}

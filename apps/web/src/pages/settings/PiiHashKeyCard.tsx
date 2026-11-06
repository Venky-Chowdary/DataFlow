import { useCallback, useEffect, useState } from "react";
import { Button } from "../../components/ui/Button";
import { useConfirm } from "../../components/ui/ConfirmDialog";
import { useToast } from "../../components/Toast";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import {
  fetchPiiHashKeyStatus,
  piiHashKeyBadge,
  piiHashKeyInputProblem,
  PII_HASH_KEY_CHANGE_WARNING,
  PII_HASH_KEY_RULE,
  rotatePiiHashKey,
  setPiiHashKey,
  type PiiHashKeyStatus,
} from "../../lib/piiHashKeyApi";

/**
 * Settings → Security: the key hash_pii digests with (QA browser 2 / security 13).
 *
 * The hash_pii refusal tells the operator to come here, so this is where a
 * workspace admin sees the key's version, rotates it, or sets their own (to
 * keep digests another system produced, or to recover a key that can no longer
 * be read). The key is never shown or returned.
 */
export function PiiHashKeyCard() {
  const manage = useWriteGate(PERMISSIONS.workspaceManage);
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const [status, setStatus] = useState<PiiHashKeyStatus | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");
  const [formError, setFormError] = useState("");

  const load = useCallback(() => {
    if (!manage.allowed) return;
    fetchPiiHashKeyStatus()
      .then((s) => { setStatus(s); setError(""); })
      .catch((err) => setError(err instanceof Error ? err.message : "Could not read the hashing key status"));
  }, [manage.allowed]);

  useEffect(load, [load]);

  const rotate = async () => {
    const ok = await confirm({
      title: "Rotate the hashing key?",
      message: PII_HASH_KEY_CHANGE_WARNING,
      confirmLabel: "Rotate key",
      cancelLabel: "Keep current key",
      tone: "danger",
    });
    if (!ok) return;
    setBusy(true);
    try {
      setStatus(await rotatePiiHashKey());
      toast({ title: "Hashing key rotated", message: "New hash_pii digests use the new version.", tone: "success" });
    } catch (err) {
      toast({ title: "Rotate failed", message: err instanceof Error ? err.message : "Request failed.", tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const save = async () => {
    const problem = piiHashKeyInputProblem(draft);
    if (problem) return void setFormError(problem);
    // Same confirmation as Rotate: setting a key changes every future digest.
    const ok = await confirm({
      title: "Set this hashing key?",
      message: PII_HASH_KEY_CHANGE_WARNING,
      confirmLabel: "Set key",
      cancelLabel: "Keep current key",
      tone: "danger",
    });
    if (!ok) return;
    setBusy(true);
    setFormError("");
    try {
      setStatus(await setPiiHashKey(draft));
      setDraft("");
      setEditing(false);
      toast({ title: "Hashing key set", message: "New hash_pii digests use the key you entered.", tone: "success" });
    } catch (err) {
      setFormError(err instanceof Error ? err.message : "Could not set the hashing key");
    } finally {
      setBusy(false);
    }
  };

  const locked = status?.managed_by_deployment === true;
  return (
    <>
      <h3 className="df2-settings-subhead df2-settings-subhead--spaced">PII hashing key</h3>
      <div className="df2-settings-policy-grid" data-testid="pii-hash-key-card">
        <div className="df2-settings-policy-row">
          <div>
            <h3>hash_pii key</h3>
            <p>
              The workspace secret hash_pii digests with. The same value always hashes the same way in this
              workspace, and differently in every other one. The key is stored encrypted and never shown.
            </p>
            {!manage.allowed && <p className="df2-settings-hint">{manage.reason}</p>}
            {error && <p className="df2-settings-hint" role="alert">{error}</p>}
            {locked && (
              <p className="df2-settings-hint">
                This deployment uses one hashing key for every workspace. Ask your platform administrator to change it.
              </p>
            )}
            {editing && !locked && (
              <div className="df2-settings-field">
                <label htmlFor="pii-hash-key-input">New hashing key</label>
                <input
                  id="pii-hash-key-input"
                  className="df2-input"
                  type="password"
                  autoComplete="off"
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                />
                <p className="df2-settings-hint">
                  {PII_HASH_KEY_RULE} Only needed to match digests produced elsewhere — otherwise Rotate.
                </p>
                {formError && <p className="df2-settings-hint" role="alert">{formError}</p>}
              </div>
            )}
          </div>
          <span className={`df2-badge ${status?.configured || locked ? "df2-badge-live" : "df2-badge-muted"}`}>
            {piiHashKeyBadge(status)}
          </span>
        </div>
      </div>
      {manage.allowed && !locked && (
        <div className="df2-settings-section-footer">
          {editing ? (
            <>
              <Button variant="primary" onClick={() => void save()} loading={busy} loadingLabel="Saving…">Save key</Button>
              <Button variant="ghost" onClick={() => { setEditing(false); setDraft(""); setFormError(""); }}>Cancel</Button>
            </>
          ) : (
            <>
              <Button variant="secondary" onClick={() => void rotate()} disabled={busy}>Rotate key</Button>
              <Button variant="ghost" onClick={() => setEditing(true)} disabled={busy}>Set my own key</Button>
            </>
          )}
        </div>
      )}
    </>
  );
}

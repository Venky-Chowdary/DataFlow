import { DtIcon } from "./DtIcon";

/**
 * States a refusal on the page, once, in the operator's own words.
 *
 * A disabled control alone leaves a viewer guessing whether the product is
 * broken. This says which authority is missing and who can grant it, so the
 * screen reads as read-only by design rather than as a failure.
 */
export function PermissionNotice({
  allowed,
  reason,
  what,
}: {
  allowed: boolean;
  /** Sentence from {@link useWriteGate} — already names the permission and role. */
  reason: string;
  /** One clause describing what is read-only here, e.g. "Connections are read-only for you." */
  what?: string;
}) {
  if (allowed || !reason) return null;
  return (
    <div className="df2-permission-notice" role="status" data-testid="permission-notice">
      <DtIcon name="shield" size={16} />
      <p>
        {what ? <strong>{what} </strong> : null}
        {reason}
      </p>
    </div>
  );
}

import { DtIcon } from "../DtIcon";
import { PLAN_CHANGED_MESSAGE } from "../../lib/planConcurrency";

/**
 * Shown when a plan save or Map was refused because the plan changed since
 * this page loaded it (409 plan_version_conflict). Nothing on screen is
 * discarded: Reload fetches the plan's latest version, and the user's edits
 * stay visible so they can be re-applied and saved on top of it.
 */
export function PlanChangedNotice({
  reloading,
  onReload,
  onDismiss,
}: {
  reloading: boolean;
  onReload: () => void;
  onDismiss: () => void;
}) {
  return (
    <div className="df2-alert df2-alert-warn" role="alert" data-testid="plan-changed-notice">
      <DtIcon name="gate" size={16} />
      <div>
        <strong>{PLAN_CHANGED_MESSAGE}</strong>
        <p>
          Your edits were not saved and are still on screen. Reload to pick up the latest version of the plan,
          then review your edits and save (Validate) again.
        </p>
        <div className="df2-btn-row">
          <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={onReload} disabled={reloading}>
            {reloading ? "Reloading…" : "Reload plan"}
          </button>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={onDismiss} disabled={reloading}>
            Dismiss
          </button>
        </div>
      </div>
    </div>
  );
}

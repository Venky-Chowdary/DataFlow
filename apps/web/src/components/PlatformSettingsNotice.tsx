import { DtIcon } from "./DtIcon";
import type { PlatformLock } from "../lib/platformSettings";

/**
 * Says why a deployment-wide settings group is read-only: it is one document
 * for every workspace, and only a platform admin changes it.
 */
export function PlatformSettingsNotice({ lock, what }: { lock: PlatformLock; what: string }) {
  if (!lock.locked) return null;
  return (
    <div className="df2-permission-notice" role="status" data-testid="platform-settings-locked">
      <DtIcon name="shield" size={16} />
      <p>
        <strong>{what} </strong>
        {lock.reason}
      </p>
    </div>
  );
}

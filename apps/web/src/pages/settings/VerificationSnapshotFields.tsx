import type { SnapshotMode } from "../../lib/verificationSnapshot";

type Props = {
  mode: SnapshotMode;
  minutes: string;
  onMode: (mode: SnapshotMode) => void;
  onMinutes: (minutes: string) => void;
};

/** This workspace's "Consistent snapshot during verification" (Parity at Scale). */
export function VerificationSnapshotFields({ mode, minutes, onMode, onMinutes }: Props) {
  return (
    <div className="df2-settings-grid df2-settings-grid--row df2-team-spaced">
      <div className="df2-settings-field">
        <label htmlFor="tenant-verification-snapshot">Consistent snapshot during verification</label>
        <select
          id="tenant-verification-snapshot"
          className="df2-select"
          data-testid="tenant-verification-snapshot"
          value={mode}
          onChange={(e) => onMode(e.target.value as SnapshotMode)}
        >
          <option value="default">Deployment default</option>
          <option value="on">On for this workspace</option>
          <option value="off">Off for this workspace</option>
        </select>
        <p className="df2-settings-hint">
          Verification of this workspace&apos;s runs reads each database from one snapshot (needed for a proven
          result). It holds an open read-only transaction while every row is compared, and lets it go at once if
          another session waits on the table. A schedule can override this.
        </p>
      </div>
      <div className="df2-settings-field">
        <label htmlFor="tenant-verification-snapshot-minutes">Longest snapshot hold (minutes)</label>
        <input
          id="tenant-verification-snapshot-minutes"
          className="df2-input"
          type="number"
          min={1}
          max={1440}
          placeholder="Deployment cap"
          value={minutes}
          onChange={(e) => onMinutes(e.target.value)}
        />
        <p className="df2-settings-hint">Never longer than the deployment&apos;s cap; empty uses the cap.</p>
      </div>
    </div>
  );
}

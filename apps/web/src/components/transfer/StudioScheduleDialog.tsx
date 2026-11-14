import { useEffect, useId, useState } from "react";

import {
  appendWithoutCursorWarning,
  appendsWithoutCursor,
  studioScheduleModeLabel,
  studioScheduleSubmitLabel,
  type StudioScheduleCadence,
  type StudioScheduleDraft,
} from "../../lib/studioScheduleDraft";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { Dialog } from "../ui/Dialog";

/**
 * UX-3: the schedule form Studio opens from "Schedule". Prefilled from the
 * route, paused by default, and explicit about an append with no cursor.
 */
export function StudioScheduleDialog({
  open,
  initial,
  route,
  syncMode,
  cursorColumn,
  saving,
  onCancel,
  onCreate,
}: {
  open: boolean;
  initial: StudioScheduleDraft;
  route: { source: string; destination: string };
  syncMode: string;
  cursorColumn: string;
  saving?: boolean;
  onCancel: () => void;
  onCreate: (draft: StudioScheduleDraft) => void;
}) {
  const [draft, setDraft] = useState(initial);
  const nameId = useId();
  const cadenceId = useId();
  const pausedId = useId();
  useEffect(() => {
    if (open) setDraft(initial);
    // Reset only when the dialog opens for a new route.
  }, [open, initial]);

  const risky = appendsWithoutCursor(syncMode, cursorColumn);
  const nameMissing = !draft.name.trim();

  return (
    <Dialog
      open={open}
      onClose={onCancel}
      size="md"
      title="Schedule this transfer"
      subtitle="Runs this Validate-approved route again on a cadence. Review it before it runs on its own."
      className="df2-studio-schedule-dialog"
      footer={
        <>
          <Button variant="secondary" onClick={onCancel} disabled={saving}>
            Cancel
          </Button>
          <Button
            variant="primary"
            loading={saving}
            loadingLabel="Creating…"
            disabled={nameMissing}
            onClick={() => onCreate({ ...draft, name: draft.name.trim() })}
            data-testid="studio-schedule-create"
          >
            {studioScheduleSubmitLabel(draft)}
          </Button>
        </>
      }
    >
      <div className="df2-studio-schedule-form">
        <dl className="df2-studio-schedule-facts">
          <div>
            <dt>Route</dt>
            <dd>
              {route.source} → {route.destination}
            </dd>
          </div>
          <div>
            <dt>Sync mode</dt>
            <dd>{studioScheduleModeLabel(syncMode)}{cursorColumn.trim() ? ` · cursor ${cursorColumn.trim()}` : ""}</dd>
          </div>
        </dl>

        {risky && (
          <div className="df2-alert df2-alert-error" role="alert" data-testid="studio-schedule-append-warning">
            <DtIcon name="warning" size={16} />
            <p>{appendWithoutCursorWarning(syncMode, draft.interval)}</p>
          </div>
        )}

        <label className="df2-label" htmlFor={nameId}>
          Schedule name
        </label>
        <input
          id={nameId}
          className="df2-input"
          value={draft.name}
          maxLength={128}
          onChange={(e) => setDraft((d) => ({ ...d, name: e.target.value }))}
        />

        <label className="df2-label" htmlFor={cadenceId}>
          Runs
        </label>
        <select
          id={cadenceId}
          className="df2-input"
          value={draft.interval}
          onChange={(e) => setDraft((d) => ({ ...d, interval: e.target.value as StudioScheduleCadence }))}
        >
          <option value="hourly">Every hour</option>
          <option value="daily">Every day</option>
          <option value="weekly">Every week</option>
        </select>

        <label className="df2-checkbox-row" htmlFor={pausedId}>
          <input
            id={pausedId}
            type="checkbox"
            checked={!draft.enabled}
            onChange={(e) => setDraft((d) => ({ ...d, enabled: !e.target.checked }))}
          />
          <span>
            Create it paused
            <small>Nothing runs until you turn it on in Schedules.</small>
          </span>
        </label>
      </div>
    </Dialog>
  );
}

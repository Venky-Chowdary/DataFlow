import { useState } from "react";
import { Button } from "../ui/Button";
import {
  destinationRecoveryOffered,
  type DestinationRecoveryAction,
} from "../../lib/scheduleRunStatus";
import type { PipelineSchedule } from "../../lib/types";

interface ScheduleHeldPanelProps {
  schedule: PipelineSchedule;
  /** Why changing this schedule is refused, when it is. */
  manageRefusal?: string;
  /** Answer a DESTINATION_CONTINUITY finding (audited, re-arms the schedule). */
  onRecoverDestination?: (action: DestinationRecoveryAction, reason: string) => void | Promise<void>;
}

/**
 * A stalled schedule says why it will not run and offers the action that
 * clears it (QA C9 / Mongo D9). A dropped or externally changed append
 * destination gets its two recoveries here — the same ones REST
 * (`POST /schedules/{id}/recover`) and MCP (`recover_schedule_destination`) offer.
 */
export function ScheduleHeldPanel({ schedule: sched, manageRefusal = "", onRecoverDestination }: ScheduleHeldPanelProps) {
  const [reason, setReason] = useState("");
  const [busy, setBusy] = useState<DestinationRecoveryAction | null>(null);
  const offered = destinationRecoveryOffered(sched) && Boolean(onRecoverDestination);
  const blocked = Boolean(manageRefusal) || !reason.trim() || Boolean(busy);

  const recover = async (action: DestinationRecoveryAction) => {
    if (!onRecoverDestination || !reason.trim()) return;
    setBusy(action);
    try {
      await onRecoverDestination(action, reason.trim());
      setReason("");
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="df2-approval-grant" role="alert" aria-label="Why this schedule is held">
      <p>
        <strong>Held — this schedule will not run on its own.</strong>{" "}
        {sched.stalled_reason || "Resolve its open finding to re-arm it."}
      </p>
      {offered && (
        <div role="group" aria-label="Recover destination">
          <label>
            <span>Reason (audited)</span>{" "}
            <input
              type="text"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder="Why the destination changed"
              aria-label="Recovery reason"
              disabled={Boolean(manageRefusal)}
            />
          </label>{" "}
          <Button
            size="sm"
            variant="secondary"
            loading={busy === "rebuild"}
            disabled={blocked}
            title={manageRefusal || "Reload the destination from the source with a full re-sync (with no key this replaces the table — you confirm first), then continue the cadence"}
            onClick={() => void recover("rebuild")}
          >
            Rebuild from source
          </Button>{" "}
          <Button
            size="sm"
            variant="ghost"
            loading={busy === "rebaseline"}
            disabled={blocked}
            title={manageRefusal || "Accept the destination as it is now (or its removal); the next run appends from the cursor"}
            onClick={() => void recover("rebaseline")}
          >
            Accept as it is (rebaseline)
          </Button>
        </div>
      )}
    </div>
  );
}

import { useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { PageSection } from "../ui/PageSection";
import { useToast } from "../Toast";
import { approveScheduleFinding, rejectScheduleFinding } from "../../lib/api";
import type { ScheduleApprovalInboxItem } from "../../lib/types";

const SCOPE_ACK: Record<string, "compliance" | "schema_drift" | "fk_risk"> = {
  replay_compliance_ack: "compliance",
  replay_schema_drift_ack: "schema_drift",
  replay_fk_risk_ack: "fk_risk",
};

const SCOPE_LABEL: Record<string, string> = {
  replay_compliance_ack: "Compliance reviewed",
  replay_schema_drift_ack: "Schema change reviewed",
  replay_fk_risk_ack: "Foreign-key risk accepted",
  net_additive_drift: "Mapped drop/rename may proceed",
};

const MIN_REASON = 8;

interface ApprovalInboxProps {
  items: ScheduleApprovalInboxItem[];
  /** Reload schedules after a decision so the row leaves this panel. */
  onDecided: () => void | Promise<void>;
  onOpenSchedule?: (scheduleId: string) => void;
}

/**
 * The decisions unattended runs are waiting on.
 *
 * A gate that refuses a scheduled run refuses identically on every later beat, so
 * the schedule parks here instead of failing nightly. Two answers are deliberately
 * separate: approving this run, and delegating the same signature to every later
 * run of the identical plan — the second stops applying the moment the mapping,
 * source shape or policy moves.
 */
export function ApprovalInbox({ items, onDecided, onOpenSchedule }: ApprovalInboxProps) {
  const { toast } = useToast();
  const [openId, setOpenId] = useState<string | null>(null);
  const [reason, setReason] = useState("");
  const [standing, setStanding] = useState(false);
  const [days, setDays] = useState(30);
  const [busy, setBusy] = useState(false);

  if (items.length === 0) return null;

  const start = (approvalId: string) => {
    setOpenId((prev) => (prev === approvalId ? null : approvalId));
    setReason("");
    setStanding(false);
    setDays(30);
  };

  const decide = async (item: ScheduleApprovalInboxItem, approve: boolean) => {
    const trimmed = reason.trim();
    if (trimmed.length < MIN_REASON) {
      toast({
        title: "A decision needs a reason",
        message: `Record at least ${MIN_REASON} characters — this is the audit trail.`,
        tone: "warning",
      });
      return;
    }
    setBusy(true);
    try {
      if (approve) {
        const acks: Record<string, boolean> = {};
        for (const scope of item.approval.requested_scopes || []) {
          const ack = SCOPE_ACK[scope];
          if (ack) acks[ack] = true;
        }
        const result = await approveScheduleFinding(item.schedule_id, item.approval.id, {
          reason: trimmed,
          ...acks,
          grant_standing: standing,
          expires_in_days: days,
        });
        toast({
          title: standing ? "Authorized" : "Approved for this run",
          message: standing
            ? `Later identical runs of “${item.schedule_name}” proceed until ${new Date(
                result.authorization.expires_at,
              ).toLocaleDateString()}.`
            : `“${item.schedule_name}” will retry now.`,
          tone: "success",
        });
      } else {
        await rejectScheduleFinding(item.schedule_id, item.approval.id, {
          reason: trimmed,
          disable: true,
        });
        toast({
          title: "Rejected",
          message: `“${item.schedule_name}” is paused rather than left to refuse again.`,
          tone: "success",
        });
      }
      setOpenId(null);
      setReason("");
      await onDecided();
    } catch (err) {
      toast({
        title: approve ? "Could not approve this finding" : "Could not reject this finding",
        message: err instanceof Error ? err.message : undefined,
        tone: "error",
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <PageSection
      className="df2-approval-inbox"
      title="Waiting on a decision"
      subtitle={`${items.length} schedule${items.length === 1 ? "" : "s"} parked on a finding rather than retrying it`}
    >
      <ul className="df2-approval-list" role="list">
        {items.map((item) => {
          const appr = item.approval;
          const expanded = openId === appr.id;
          return (
            <li key={`${item.schedule_id}:${appr.id}`} className="df2-approval-row">
              <div className="df2-approval-head">
                <span className="df2-approval-code" title="Refusal code">
                  <DtIcon name="alert" size={14} />
                  {appr.code || "RUN_REFUSED"}
                </span>
                <button
                  type="button"
                  className="df2-approval-name"
                  onClick={() => onOpenSchedule?.(item.schedule_id)}
                  title="Open this schedule"
                >
                  {item.schedule_name}
                </button>
                <span className="df2-approval-route" title={`${item.source} → ${item.destination}`}>
                  {item.source} → {item.destination}
                </span>
                {appr.occurrences > 1 && (
                  <span className="df2-approval-count" title="Times this finding was seen">
                    seen {appr.occurrences}×
                  </span>
                )}
              </div>

              <p className="df2-approval-finding">{appr.finding}</p>
              {appr.corrective_action && (
                <p className="df2-approval-action">
                  <strong>To resolve:</strong> {appr.corrective_action}
                </p>
              )}

              {!appr.approvable ? (
                <p className="df2-approval-nonapprovable">
                  No signature can clear this. It is a plan change, not a decision —
                  approving it would only refuse again on the next run.
                </p>
              ) : expanded ? (
                <div className="df2-approval-form">
                  <label className="df2-approval-field">
                    <span>Reason (recorded in the audit trail)</span>
                    <textarea
                      className="df2-input"
                      rows={2}
                      value={reason}
                      onChange={(e) => setReason(e.target.value)}
                      placeholder="Why this change is accepted, and who accepted it with you."
                    />
                  </label>
                  <ul className="df2-approval-scopes" role="list">
                    {(appr.requested_scopes || []).map((scope) => (
                      <li key={scope}>{SCOPE_LABEL[scope] ?? scope}</li>
                    ))}
                  </ul>
                  <label className="df2-approval-standing">
                    <input
                      type="checkbox"
                      checked={standing}
                      onChange={(e) => setStanding(e.target.checked)}
                    />
                    <span>
                      Also authorize later identical runs
                      <em>
                        Stops applying if the mapping, source shape, policy or contract
                        changes.
                      </em>
                    </span>
                  </label>
                  {standing && (
                    <label className="df2-approval-field df2-approval-expiry">
                      <span>Expires in (days)</span>
                      <input
                        className="df2-input"
                        type="number"
                        min={1}
                        max={90}
                        value={days}
                        onChange={(e) => setDays(Number(e.target.value) || 30)}
                      />
                    </label>
                  )}
                  <div className="df2-approval-actions">
                    <Button
                      size="sm"
                      variant="primary"
                      loading={busy}
                      onClick={() => void decide(item, true)}
                    >
                      {standing ? "Approve and authorize" : "Approve this run"}
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      loading={busy}
                      onClick={() => void decide(item, false)}
                    >
                      Reject and pause
                    </Button>
                    <Button size="sm" variant="ghost" onClick={() => setOpenId(null)}>
                      Cancel
                    </Button>
                  </div>
                </div>
              ) : (
                <div className="df2-approval-actions">
                  <Button size="sm" variant="primary" onClick={() => start(appr.id)}>
                    Decide
                  </Button>
                </div>
              )}
            </li>
          );
        })}
      </ul>
    </PageSection>
  );
}

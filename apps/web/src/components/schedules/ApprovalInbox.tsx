import { useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { PageSection } from "../ui/PageSection";
import { useToast } from "../Toast";
import { acceptScheduleSourceSchema, approveScheduleFinding, rejectScheduleFinding } from "../../lib/api";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import { inboxCorrectiveAction, inboxNeedsRunNow, inboxNeedsStudio } from "../../lib/scheduleApprovalCta";
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
  /** Render the selected schedule's finding inside its detail drawer. */
  compact?: boolean;
  /** Reload schedules after a decision so the row leaves this panel. */
  onDecided: () => void | Promise<void>;
  onOpenSchedule?: (scheduleId: string) => void;
  /** Open the schedule editor / Map so a drift finding can be reviewed. */
  onReviewMapping?: (scheduleId: string) => void;
  /** Open Transfer Studio with this schedule's route — empty-mapping plan change. */
  onOpenStudio?: (scheduleId: string) => void;
  /** Run now — dest-exists after create-new is not a signature. */
  onRunNow?: (scheduleId: string) => void | Promise<void>;
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
function isSourceSchemaDrift(item: ScheduleApprovalInboxItem): boolean {
  const code = (item.approval.code || "").toUpperCase();
  const kind = (item.approval.kind || "").toLowerCase();
  return code === "SOURCE_SCHEMA_DRIFT" || kind === "source_drift";
}

export function ApprovalInbox({ items, compact = false, onDecided, onOpenSchedule, onReviewMapping, onOpenStudio, onRunNow }: ApprovalInboxProps) {
  const { toast } = useToast();
  const authorize = useWriteGate(PERMISSIONS.scheduleAuthorize);
  const manage = useWriteGate(PERMISSIONS.scheduleManage);
  const [openId, setOpenId] = useState<string | null>(null);
  const [reason, setReason] = useState("");
  const [standing, setStanding] = useState(false);
  const [days, setDays] = useState(30);
  const [busy, setBusy] = useState(false);
  const [acceptingId, setAcceptingId] = useState<string | null>(null);
  const [minimized, setMinimized] = useState(false);
  const [dismissed, setDismissed] = useState(false);
  const inboxVersion = items
    .map((item) => `${item.schedule_id}:${item.approval.id}:${item.approval.occurrences}:${item.approval.last_seen_at || ""}`)
    .sort()
    .join("|");

  useEffect(() => {
    setMinimized(false);
    setDismissed(false);
  }, [inboxVersion]);

  if (items.length === 0) return null;

  const start = (approvalId: string) => {
    setOpenId((prev) => (prev === approvalId ? null : approvalId));
    setReason("");
    setStanding(false);
    setDays(30);
  };

  const acceptSource = async (item: ScheduleApprovalInboxItem) => {
    if (!manage.allowed) {
      toast({ title: "No write permission", message: manage.reason, tone: "warning" });
      return;
    }
    setAcceptingId(item.schedule_id);
    try {
      const res = await acceptScheduleSourceSchema(item.schedule_id);
      toast({
        title: "Baseline recorded",
        message: res.message || `“${item.schedule_name}” will compare against the current source shape.`,
        tone: "success",
      });
      await onDecided();
    } catch (err) {
      toast({
        title: "Could not record the source shape",
        message: err instanceof Error ? err.message : undefined,
        tone: "error",
      });
    } finally {
      setAcceptingId(null);
    }
  };

  const decide = async (item: ScheduleApprovalInboxItem, approve: boolean) => {
    if (!authorize.allowed) {
      toast({ title: "No write permission", message: authorize.reason, tone: "warning" });
      return;
    }
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

  const sectionActions = compact ? undefined : dismissed ? (
    <Button size="sm" variant="secondary" onClick={() => setDismissed(false)}>
      Show decisions ({items.length})
    </Button>
  ) : (
    <div className="df2-approval-inbox-controls">
      <Button size="sm" variant="ghost" onClick={() => setMinimized((value) => !value)}>
        {minimized ? "Expand" : "Minimize"}
      </Button>
      <Button
        size="sm"
        variant="ghost"
        aria-label="Close waiting on a decision panel"
        title="Hide this panel; findings remain open"
        onClick={() => setDismissed(true)}
      >
        <DtIcon name="x" size={14} />
      </Button>
    </div>
  );

  return (
    <PageSection
      className={`df2-approval-inbox${compact ? " is-compact" : ""}${dismissed ? " is-dismissed" : ""}`}
      asCard={!compact && !dismissed}
      title={compact ? "Decision required" : "Waiting on a decision"}
      subtitle={compact
        ? `${items.length} finding${items.length === 1 ? "" : "s"} currently blocks this schedule.`
        : `${items.length} schedule${items.length === 1 ? "" : "s"} parked on a finding rather than retrying it`}
      actions={sectionActions}
    >
      {dismissed ? (
        <p className="df2-approval-dismissed" role="status">
          The decision inbox is hidden for now. Parked schedules remain open and are shown in their detail drawers.
        </p>
      ) : minimized ? (
        <p className="df2-approval-minimized" role="status">
          {items.length} open decision{items.length === 1 ? "" : "s"} · select a schedule to inspect its finding and next action.
        </p>
      ) : <ul className="df2-approval-list" role="list">
        {items.map((item) => {
          const appr = item.approval;
          const expanded = openId === appr.id;
          const corrective = inboxCorrectiveAction(item);
          return (
            <li key={`${item.schedule_id}:${appr.id}`} className="df2-approval-row">
              <div className="df2-approval-head">
                <span className="df2-approval-code" title="Refusal code">
                  <DtIcon name="alert" size={14} />
                  {appr.code || "RUN_REFUSED"}
                </span>
                {onOpenSchedule ? (
                  <button
                    type="button"
                    className="df2-approval-name"
                    onClick={() => onOpenSchedule(item.schedule_id)}
                    title="Open this schedule"
                  >
                    {item.schedule_name}
                  </button>
                ) : <strong className="df2-approval-name">{item.schedule_name}</strong>}
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
              {corrective && (
                <p className="df2-approval-action">
                  <strong>To resolve:</strong> {corrective}
                </p>
              )}

              {!appr.approvable && inboxNeedsRunNow(item) ? (
                <div className="df2-approval-plan-change">
                  <p className="df2-approval-nonapprovable">
                    Dest existing after the first write is not a plan change. A signature
                    cannot help — Run now if Map is unchanged.
                  </p>
                  {onRunNow && (
                    <div className="df2-approval-actions">
                      <Button
                        size="sm"
                        variant="primary"
                        onClick={() => void onRunNow(item.schedule_id)}
                      >
                        Run now
                      </Button>
                    </div>
                  )}
                </div>
              ) : !appr.approvable ? (
                <div className="df2-approval-plan-change">
                  <p className="df2-approval-nonapprovable">
                    No signature can clear this. It is a plan change, not a decision —
                    approving it would only refuse again on the next run.
                  </p>
                  {inboxNeedsStudio(item) && onOpenStudio && (
                    <div className="df2-approval-actions">
                      <Button
                        size="sm"
                        variant="primary"
                        onClick={() => onOpenStudio(item.schedule_id)}
                      >
                        Open Transfer Studio
                      </Button>
                    </div>
                  )}
                  {isSourceSchemaDrift(item) && (
                    <div className="df2-approval-actions">
                      {onReviewMapping && (
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() => onReviewMapping(item.schedule_id)}
                        >
                          Review mapping
                        </Button>
                      )}
                      <Button
                        size="sm"
                        variant="primary"
                        loading={acceptingId === item.schedule_id}
                        disabled={!manage.allowed || acceptingId === item.schedule_id}
                        title={manage.reason || "Record the source shape this finding already compared"}
                        onClick={() => void acceptSource(item)}
                      >
                        {acceptingId === item.schedule_id
                          ? "Recording…"
                          : "Accept new source shape"}
                      </Button>
                    </div>
                  )}
                </div>
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
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={!authorize.allowed}
                    title={authorize.reason || undefined}
                    onClick={() => start(appr.id)}
                  >
                    Decide
                  </Button>
                  {!authorize.allowed && (
                    <span className="df2-approval-nonapprovable">{authorize.reason}</span>
                  )}
                </div>
              )}
            </li>
          );
        })}
      </ul>
      }
    </PageSection>
  );
}

import { useMemo, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { Drawer } from "../ui/Drawer";
import {
  decideRepairProposal,
  type RepairMapping,
  type RepairProposal,
} from "../../lib/api";

export interface RepairProposalDrawerProps {
  open: boolean;
  proposal: RepairProposal | null;
  mappings?: RepairMapping[];
  actor?: string;
  onClose: () => void;
  /** Called after approve+apply with updated mappings (when mappings were supplied). */
  onApplied?: (updated: RepairMapping[], proposal: RepairProposal) => void;
  /** Called after reject, audit approve, or continue-without-decide. */
  onDecided?: (proposal: RepairProposal) => void;
  /** Preferred for duplicate identity keys — Destination Advanced (PK / sync mode). */
  onOpenIdentitySettings?: () => void;
  /** Fallback navigate to Map for identity / DDL review (non-mutative proposals). */
  onOpenMap?: () => void;
}

const MUTATIVE_KINDS = new Set(["change_target_type", "add_transform", "map_column"]);

function isMutativeAction(action: Record<string, unknown>): boolean {
  const kind = String(action.kind || "");
  if (!MUTATIVE_KINDS.has(kind)) return false;
  if (action.mapping_applyable === false) return false;
  return Boolean(action.column || action.source);
}

function isIsoWarningIssue(issue: Record<string, unknown>): boolean {
  const blob = `${issue.title || ""} ${issue.what || ""} ${issue.fix || ""}`;
  return /type normalize at write/i.test(String(issue.title || ""))
    || (/ISO timestamps?/i.test(blob) && /normaliz/i.test(blob));
}

/**
 * Human-gated agentic repair: review proposed actions, approve/reject with audit trail.
 * Approve with mappings runs the real ``apply_actions_to_mappings`` path on the API.
 * Without mutative actions (e.g. duplicate identity keys), Approve & apply is disabled.
 */
export function RepairProposalDrawer({
  open,
  proposal,
  mappings = [],
  actor = "operator",
  onClose,
  onApplied,
  onDecided,
  onOpenIdentitySettings,
  onOpenMap,
}: RepairProposalDrawerProps) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const analysis = useMemo(() => {
    if (!proposal) {
      return {
        mutativeActions: [] as Record<string, unknown>[],
        guidanceActions: [] as Record<string, unknown>[],
        blockers: [] as Record<string, unknown>[],
        warnings: [] as Record<string, unknown>[],
        duplicateRoot: false,
        canMutateMappings: false,
      };
    }
    const actions = proposal.actions || [];
    const mutativeActions = actions.filter(isMutativeAction);
    const guidanceActions = actions.filter((a) => !isMutativeAction(a));
    const issues = Array.isArray(proposal.diagnosis?.issues)
      ? (proposal.diagnosis!.issues as Record<string, unknown>[])
      : [];
    const blockers = issues.filter((i) => i.severity === "block" || i.severity === "error");
    const warnings = issues.filter(
      (i) => i.severity === "warning" || i.severity === "warn" || isIsoWarningIssue(i),
    );
    const duplicateRoot =
      proposal.diagnosis?.root_cause === "duplicate_identity_keys"
      || actions.some((a) => a.kind === "fix_source_keys")
      || Boolean(proposal.diagnosis?.mapping_applyable === false && /duplicate/i.test(proposal.summary || ""));
    return {
      mutativeActions,
      guidanceActions,
      blockers,
      warnings: warnings.filter((w) => !blockers.includes(w)),
      duplicateRoot,
      canMutateMappings: mutativeActions.length > 0,
    };
  }, [proposal]);

  if (!proposal) return null;

  const canApplyMappings = mappings.length > 0 && analysis.canMutateMappings;
  const status = proposal.status;
  const canDecide =
    status === "proposed"
    || (status === "approved" && canApplyMappings);
  const canReject = status === "proposed" || status === "approved";

  const decide = async (approve: boolean) => {
    setBusy(true);
    setError(null);
    try {
      const decided = await decideRepairProposal(proposal.id, {
        approve,
        actor,
        mappings: approve && canApplyMappings ? mappings : undefined,
      });
      if (
        approve
        && decided.apply_result?.applied
        && decided.apply_result?.mappings
        && Array.isArray(decided.apply_result.mappings)
      ) {
        onApplied?.(decided.apply_result.mappings as RepairMapping[], decided);
      } else if (approve && decided.apply_result && decided.apply_result.applied === false) {
        setError(
          String(
            decided.apply_result.message
            || decided.apply_result.reason
            || "No mapping changes applied — this proposal cannot auto-fix the blocker.",
          ),
        );
        onDecided?.(decided);
        return;
      } else {
        onDecided?.(decided);
      }
      onClose();
    } catch (e) {
      setError((e as Error).message || "Repair decision failed");
    } finally {
      setBusy(false);
    }
  };

  /** No mappings yet — do not audit-approve (that would lock Apply). Hand off to Validate. */
  const continueWithoutDecide = () => {
    onDecided?.(proposal);
    onClose();
  };

  return (
    <Drawer
      open={open}
      onClose={onClose}
      size="lg"
      title="Repair proposal"
      subtitle={
        analysis.canMutateMappings
          ? "Human-gated fix — nothing auto-applies without approve."
          : "Guidance only — mapping Approve cannot fix this root cause."
      }
      icon={<DtIcon name="sparkle" size={18} />}
      footer={
        <div className="df2-bad-data-footer">
          <Button variant="ghost" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button
            variant="secondary"
            onClick={() => void decide(false)}
            disabled={busy || !canReject}
          >
            Reject
          </Button>
          {analysis.duplicateRoot && (onOpenIdentitySettings || onOpenMap) ? (
            <Button
              variant="primary"
              onClick={() => {
                if (onOpenIdentitySettings) onOpenIdentitySettings();
                else onOpenMap?.();
                onClose();
              }}
              disabled={busy}
              leadingIcon={<DtIcon name={onOpenIdentitySettings ? "settings" : "layers"} size={14} />}
            >
              {onOpenIdentitySettings
                ? "Change primary key or sync mode"
                : "Open Map — review identity"}
            </Button>
          ) : canApplyMappings ? (
            <Button
              variant="primary"
              onClick={() => void decide(true)}
              loading={busy}
              loadingLabel="Applying…"
              disabled={!canDecide}
              leadingIcon={<DtIcon name="check" size={14} />}
            >
              Approve & apply mappings
            </Button>
          ) : mappings.length > 0 ? (
            <Button
              variant="primary"
              onClick={continueWithoutDecide}
              disabled={busy || status === "rejected" || status === "applied"}
              leadingIcon={<DtIcon name="gate" size={14} />}
            >
              Continue in Validate
            </Button>
          ) : (
            <Button
              variant="primary"
              onClick={continueWithoutDecide}
              disabled={busy || status === "rejected" || status === "applied"}
              leadingIcon={<DtIcon name="gate" size={14} />}
            >
              Continue in Validate
            </Button>
          )}
        </div>
      }
    >
      <div className="df2-repair-proposal">
        <p className="df2-label-hint" style={{ marginTop: 0 }}>
          Proposal <code>{proposal.id}</code>
          {proposal.job_id ? <> · job <code>{proposal.job_id}</code></> : null}
          {" · "}
          source <strong>{proposal.source}</strong>
          {" · "}
          confidence <strong>{proposal.confidence}</strong>
          {" · "}
          status <strong>{proposal.status}</strong>
        </p>

        {analysis.duplicateRoot && (
          <div className="df2-repair-root" role="status">
            <strong>Root cause: duplicate identity keys</strong>
            <p>
              Source sample has colliding values on the identity column (usually <code>id</code>).
              Data integrity, Target DDL, and Sample reconciliation all fail for that same reason.
              Approve &amp; apply cannot dedupe rows — fix the data or identity contract, then Re-run Validate.
            </p>
            <ol className="df2-repair-next-steps">
              <li>Dedupe the MySQL source on the real unique key, or</li>
              <li>On Map, stop treating a non-unique column as identity (pick another unique field), or</li>
              <li>Change sync mode only if the destination truly allows non-unique loads — then Re-run Validate.</li>
            </ol>
          </div>
        )}

        {!analysis.duplicateRoot && proposal.summary && (
          <div className="df2-repair-summary">
            {String(proposal.summary).split("\n").filter(Boolean).map((line, i) => (
              <p key={i}>{line}</p>
            ))}
          </div>
        )}

        {analysis.blockers.length > 0 && (
          <div className="df2-repair-section">
            <h4 className="df2-label">Blockers</h4>
            <ul className="df2-repair-issues">
              {analysis.blockers.map((issue, i) => (
                <li key={`b-${i}`} className="is-block">
                  <strong>{String(issue.title || issue.gate || "Blocker")}</strong>
                  {issue.what ? <p>{String(issue.what)}</p> : null}
                  {issue.fix ? <p className="df2-label-hint">Fix: {String(issue.fix)}</p> : null}
                </li>
              ))}
            </ul>
          </div>
        )}

        {analysis.warnings.length > 0 && (
          <div className="df2-repair-section">
            <h4 className="df2-label">Warnings (not blockers)</h4>
            <ul className="df2-repair-issues">
              {analysis.warnings.map((issue, i) => (
                <li key={`w-${i}`} className="is-warn">
                  <strong>{String(issue.title || "Warning")}</strong>
                  {Array.isArray(issue.columns) && issue.columns.length > 0 ? (
                    <p className="df2-label-hint">{(issue.columns as string[]).join(", ")}</p>
                  ) : issue.what ? (
                    <p className="df2-label-hint">{String(issue.what).slice(0, 160)}</p>
                  ) : null}
                </li>
              ))}
            </ul>
          </div>
        )}

        {proposal.auto_applicable && (
          <p className="df2-label-hint" role="status">
            Marked auto-applicable (additive only) — still requires explicit approve in Studio.
          </p>
        )}
        {error && (
          <p className="df2-label-hint" role="alert" style={{ color: "var(--df2-danger, #b42318)" }}>
            {error}
          </p>
        )}

        {analysis.mutativeActions.length > 0 && (
          <>
            <h4 className="df2-label">Applyable mapping actions ({analysis.mutativeActions.length})</h4>
            <ul className="df2-repair-actions">
              {analysis.mutativeActions.map((a, i) => (
                <li key={`m-${String(a.kind)}-${String(a.column ?? "")}-${i}`}>
                  <strong>{String(a.kind || "action")}</strong>
                  {a.column ? <> · <code>{String(a.column)}</code></> : null}
                  {a.to_type ? <> → {String(a.to_type)}</> : null}
                  {a.transform ? <> · transform {String(a.transform)}</> : null}
                  {a.label ? <span className="df2-label-hint"> — {String(a.label)}</span> : null}
                </li>
              ))}
            </ul>
          </>
        )}

        {analysis.guidanceActions.length > 0 && (
          <>
            <h4 className="df2-label">Guidance ({analysis.guidanceActions.length})</h4>
            <ul className="df2-repair-actions">
              {analysis.guidanceActions.map((a, i) => (
                <li key={`g-${String(a.kind)}-${i}`}>
                  <strong>{String(a.kind || "action")}</strong>
                  {a.label ? <span className="df2-label-hint"> — {String(a.label)}</span> : null}
                </li>
              ))}
            </ul>
          </>
        )}

        {proposal.actions.length === 0 && (
          <p className="df2-label-hint">No concrete actions — review diagnosis and fix mappings manually.</p>
        )}

        {canApplyMappings ? (
          <p className="df2-label-hint">
            {mappings.length} Studio mapping(s) attached — Approve will apply the mutative actions above
            {status === "approved" ? " (proposal was audit-approved earlier)" : ""}
            {" "}and then open Validate so you can re-run gates.
          </p>
        ) : (
          <p className="df2-label-hint">
            {analysis.duplicateRoot
              ? "Approve & apply mappings is disabled for this proposal — it would not clear duplicate identity keys."
              : "No mapping mutations in this proposal — Continue returns to Validate without pretending a fix applied."}
          </p>
        )}
      </div>
    </Drawer>
  );
}

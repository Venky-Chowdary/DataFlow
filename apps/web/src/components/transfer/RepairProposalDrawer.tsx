import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
  if (action.requires_ddl === true && kind === "change_target_type") return false;
  if (action.apply_proven === false && kind === "change_target_type") return false;
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
export function RepairProposalDrawer(props: RepairProposalDrawerProps) {
  if (!props.proposal) return null;
  return <RepairProposalState key={`${props.proposal.id}:${props.proposal.job_id || ""}`} {...props} />;
}

function RepairActionDetails({ action, manual = false }: { action: Record<string, unknown>; manual?: boolean }) {
  return (
    <>
      <div className="df2-repair-action-title">
        <DtIcon name={manual ? "settings" : "layers"} size={14} />
        <strong>
          {action.kind === "change_target_type" ? "Change target type"
            : action.kind === "add_transform" ? "Add transform"
              : action.kind === "map_column" ? "Map column"
                : action.kind === "fix_source_keys" ? "Fix source keys"
                  : "Review proposed step"}
        </strong>
      </div>
      {manual && <p className="df2-repair-manual-note">Manual step. Not applied by this proposal.</p>}
      <dl className="df2-repair-action-details">
        <div><dt>Column</dt><dd><code>{String(action.column || action.source || "Not specified")}</code></dd></div>
        <div>
          <dt>{action.transform ? "Proposed transform" : "Proposed change"}</dt>
          <dd>{action.transform ? <code>{String(action.transform)}</code>
            : action.to_type ? <code>{String(action.to_type)}</code>
              : action.destination ? <code>{String(action.destination)}</code>
                : "See step details"}</dd>
        </div>
        <div><dt>Evidence</dt><dd>{action.apply_proven === true ? "Proven against scanned values"
          : action.apply_proven === false ? "Not proven" : "Unverified"}</dd></div>
      </dl>
      {action.label ? <p className="df2-repair-action-note">{String(action.label)}</p> : null}
    </>
  );
}

function RepairProposalState({
  open,
  proposal,
  mappings = [],
  onClose,
  onApplied,
  onDecided,
  onOpenIdentitySettings,
  onOpenMap,
}: RepairProposalDrawerProps) {
  const active = useRef(open);
  const generation = useRef(0);
  const decisionContext = JSON.stringify([proposal?.actions, mappings]);
  const deciding = useRef(false);
  const closeRef = useRef(onClose);
  useEffect(() => { closeRef.current = onClose; }, [onClose]);
  useEffect(() => {
    generation.current += 1;
    active.current = open;
    return () => { active.current = false; generation.current += 1; };
  }, [open, decisionContext]);
  const closeWhenIdle = useCallback(() => {
    if (!deciding.current) closeRef.current();
  }, []);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const errorRef = useRef<HTMLParagraphElement>(null);
  useEffect(() => { if (error) errorRef.current?.focus(); }, [error]);

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
    if (!active.current || deciding.current || (approve ? !canDecide : !canReject)) return;
    deciding.current = true;
    const requestGeneration = generation.current;
    const current = () => active.current && generation.current === requestGeneration;
    setBusy(true);
    setError(null);
    try {
      const decided = await decideRepairProposal(proposal.id, {
        approve,
        mappings: approve && canApplyMappings ? mappings : undefined,
      });
      if (!current()) return;
      if (decided.id !== proposal.id || (proposal.job_id && decided.job_id !== proposal.job_id)) {
        throw new Error("Repair response does not match the current proposal. Reload before applying changes.");
      }
      if (
        approve
        && decided.status === "applied"
        && decided.apply_result?.applied === true
        && decided.apply_result?.mappings
        && Array.isArray(decided.apply_result.mappings)
        && decided.apply_result.mappings.length > 0
      ) {
        onApplied?.(decided.apply_result.mappings as RepairMapping[], decided);
      } else if (approve && canApplyMappings) {
        setError(
          String(
            decided.apply_result?.message
            || decided.apply_result?.reason
            || "No mapping changes applied — this proposal cannot auto-fix the blocker.",
          ),
        );
        if (decided.status === "approved") onDecided?.(decided);
        return;
      } else {
        onDecided?.(decided);
      }
      onClose();
    } catch (e) {
      if (!current()) return;
      setError((e as Error).message || "Repair decision failed");
    } finally {
      deciding.current = false;
      if (active.current) setBusy(false);
    }
  };

  /** No mappings yet — do not audit-approve (that would lock Apply). Hand off to Validate. */
  const continueWithoutDecide = () => {
    if (!active.current || deciding.current) return;
    onDecided?.(proposal);
    onClose();
  };

  return (
    <Drawer
      open={open}
      onClose={closeWhenIdle}
      size="lg"
      title="Repair proposal"
      ariaLabel="Repair proposal"
      subtitle={
        analysis.canMutateMappings
          ? "Mapping changes pending approval"
          : "Manual repair required"
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
          {analysis.duplicateRoot && !canApplyMappings && (onOpenIdentitySettings || onOpenMap) ? (
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
              {analysis.mutativeActions.every((a) => a.kind === "change_target_type")
                ? "Approve & apply CREATE types"
                : "Approve & apply mappings"}
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
        <div className="df2-repair-status"><span>Status</span><strong>{proposal.status}</strong></div>
        <dl className="df2-repair-metadata">
          <div><dt>Proposal</dt><dd><code>{proposal.id}</code></dd></div>
          {proposal.job_id ? <div><dt>Job</dt><dd><code>{proposal.job_id}</code></dd></div> : null}
          <div><dt>Source</dt><dd>{proposal.source || "Not specified"}</dd></div>
          <div><dt>Confidence</dt><dd>{proposal.confidence ?? "Unverified"}</dd></div>
        </dl>

        {analysis.duplicateRoot && (
          <div className="df2-repair-root" role="status">
            <strong>Root cause: duplicate identity keys</strong>
            <p>
              Source sample has colliding values on the identity column (usually <code>id</code>).
              Data integrity, Target DDL, and Sample reconciliation all fail for that same reason.
              Approve &amp; apply cannot dedupe rows — fix the data or identity contract, then Re-run Validate.
            </p>
            <ol className="df2-repair-next-steps">
              <li>Dedupe the source on the real unique key, or</li>
              <li>
                Open Destination → Advanced settings and set Primary key to a unique column
                (Map Approve cannot change identity), or
              </li>
              <li>
                Switch to append/overwrite only if the destination truly allows non-unique loads —
                then Re-run Validate.
              </li>
            </ol>
            {canApplyMappings && (onOpenIdentitySettings || onOpenMap) && (
              <Button variant="secondary" size="sm" disabled={busy} onClick={() => {
                if (deciding.current) return;
                if (onOpenIdentitySettings) onOpenIdentitySettings();
                else onOpenMap?.();
                onClose();
              }} leadingIcon={<DtIcon name="settings" size={14} />}>
                Review identity separately
              </Button>
            )}
          </div>
        )}

        {!analysis.duplicateRoot && analysis.mutativeActions.some((a) => a.kind === "change_target_type") && (
          <div className="df2-repair-root" role="status">
            <strong>What Approve does</strong>
            <p>
              The source is not broken. Map guessed destination types from the preview.
              Later values do not fit those types, so CREATE would be too narrow and
              Execute would load nothing — on any warehouse or database destination.
            </p>
            <ol className="df2-repair-next-steps">
              <li>Approve updates the Map CREATE types listed below — the destination is not written yet.</li>
              <li>Evidence below identifies types proven against scanned values. Missing or unknown proof flags are unverified.</li>
              <li>Re-run Validate after approval. Execute only after the required gates pass.</li>
            </ol>
            <p className="df2-label-hint">
              Live destination columns still need ALTER — Map cannot change destination
              DDL. Approve is disabled for those. Do not truncate.
            </p>
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

        {proposal.auto_applicable === true && (
          <p className="df2-label-hint" role="status">
            Marked auto-applicable. Explicit approval is still required; this flag is not validation evidence.
          </p>
        )}
        {error && (
          <p ref={errorRef} tabIndex={-1} className="df2-label-hint" role="alert" style={{ color: "var(--df2-danger, #b42318)" }}>
            {error}
          </p>
        )}

        {analysis.mutativeActions.length > 0 && (
          <>
            <h4 className="df2-label">Proposed mapping changes ({analysis.mutativeActions.length})</h4>
            <ul className="df2-repair-actions">
              {analysis.mutativeActions.map((a, i) => (
                <li key={`m-${String(a.kind)}-${String(a.column ?? "")}-${i}`}>
                  <RepairActionDetails action={a} />
                </li>
              ))}
            </ul>
          </>
        )}

        {analysis.guidanceActions.length > 0 && (
          <>
            <h4 className="df2-label">Manual steps ({analysis.guidanceActions.length})</h4>
            <ul className="df2-repair-actions">
              {analysis.guidanceActions.map((a, i) => (
                <li key={`g-${String(a.kind)}-${i}`}>
                  <RepairActionDetails action={a} manual />
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

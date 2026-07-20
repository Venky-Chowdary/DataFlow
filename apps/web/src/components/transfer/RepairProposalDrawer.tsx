import { useState } from "react";
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
  /** Called after reject or approve-without-mappings. */
  onDecided?: (proposal: RepairProposal) => void;
}

/**
 * Human-gated agentic repair: review proposed actions, approve/reject with audit trail.
 * Approve with mappings runs the real ``apply_actions_to_mappings`` path on the API.
 */
export function RepairProposalDrawer({
  open,
  proposal,
  mappings = [],
  actor = "operator",
  onClose,
  onApplied,
  onDecided,
}: RepairProposalDrawerProps) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (!proposal) return null;

  const decide = async (approve: boolean) => {
    setBusy(true);
    setError(null);
    try {
      const decided = await decideRepairProposal(proposal.id, {
        approve,
        actor,
        mappings: approve && mappings.length ? mappings : undefined,
      });
      if (approve && decided.apply_result?.mappings && Array.isArray(decided.apply_result.mappings)) {
        onApplied?.(decided.apply_result.mappings as RepairMapping[], decided);
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

  return (
    <Drawer
      open={open}
      onClose={onClose}
      width={560}
      title="Repair proposal"
      subtitle="Human-gated fix — nothing auto-applies without approve."
      icon={<DtIcon name="sparkle" size={18} />}
      footer={
        <div className="df2-bad-data-footer">
          <Button variant="ghost" onClick={onClose} disabled={busy}>
            Cancel
          </Button>
          <Button
            variant="secondary"
            onClick={() => void decide(false)}
            disabled={busy || proposal.status !== "proposed"}
          >
            Reject
          </Button>
          <Button
            variant="primary"
            onClick={() => void decide(true)}
            loading={busy}
            loadingLabel="Applying…"
            disabled={proposal.status !== "proposed"}
            leadingIcon={<DtIcon name="check" size={14} />}
          >
            {mappings.length ? "Approve & apply" : "Approve"}
          </Button>
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
        <p>{proposal.summary}</p>
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
        <h4 className="df2-label">Actions ({proposal.actions.length})</h4>
        {proposal.actions.length === 0 ? (
          <p className="df2-label-hint">No concrete actions — review diagnosis and fix mappings manually.</p>
        ) : (
          <ul className="df2-repair-actions">
            {proposal.actions.map((a, i) => (
              <li key={`${a.kind}-${a.column ?? ""}-${i}`}>
                <strong>{String(a.kind || "action")}</strong>
                {a.column ? <> · <code>{String(a.column)}</code></> : null}
                {a.to_type ? <> → {String(a.to_type)}</> : null}
                {a.transform ? <> · transform {String(a.transform)}</> : null}
                {a.label ? <span className="df2-label-hint"> — {String(a.label)}</span> : null}
              </li>
            ))}
          </ul>
        )}
        {!mappings.length && proposal.status === "proposed" && (
          <p className="df2-label-hint">
            No Studio mappings attached — Approve records the decision in the audit trail without
            mutating the map. Open Validate with mappings to Approve &amp; apply.
          </p>
        )}
      </div>
    </Drawer>
  );
}

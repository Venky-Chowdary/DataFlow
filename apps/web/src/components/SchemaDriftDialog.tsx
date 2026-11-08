import { Button } from "./ui/Button";
import { Dialog } from "./ui/Dialog";
import { DtIcon } from "./DtIcon";
import type { SchemaDriftReport } from "../lib/api";

interface SchemaDriftDialogProps {
  open: boolean;
  report: SchemaDriftReport | null;
  onApproveAdditive?: () => void;
  onReject?: () => void;
  onRemap?: () => void;
  onClose: () => void;
  onAcknowledge?: () => void;
}

export function schemaDriftReportFromDetails(details: Record<string, unknown>, message: string): SchemaDriftReport {
  const evolution = details.schema_evolution && typeof details.schema_evolution === "object"
    ? details.schema_evolution as Record<string, unknown> : {};
  const classification = details.classification && typeof details.classification === "object"
    ? details.classification as Record<string, unknown> : {};
  const changes = (value: unknown) => Array.isArray(value) ? value
    .filter((item): item is Record<string, unknown> => item !== null && typeof item === "object")
    .map((item) => ({
      kind: typeof item.kind === "string" ? item.kind : "unknown_change",
      column: typeof item.column === "string" ? item.column : typeof item.source === "string" ? item.source : undefined,
      to: typeof item.to === "string" ? item.to : undefined,
      from_type: typeof item.old_type === "string" ? item.old_type : typeof item.from_type === "string" ? item.from_type : undefined,
      to_type: typeof item.new_type === "string" ? item.new_type : typeof item.to_type === "string" ? item.to_type : undefined,
    })) : [];
  const hard = [...changes(details.hard_breaking), ...changes(evolution.hard_breaking), ...changes(classification.hard_breaking)];
  const unique = (items: ReturnType<typeof changes>) => items.filter((item, index, all) => all.findIndex(
    (candidate) => candidate.kind === item.kind && candidate.column === item.column,
  ) === index);
  const additive = unique([...changes(classification.additive), ...changes(evolution.additive), ...changes(details.additive)]);
  const breaking = unique([...changes(classification.breaking), ...changes(details.breaking), ...changes(evolution.soft_net_additive)]);
  return {
    severity: hard.length ? "breaking" : additive.length ? "additive" : "review",
    additive, breaking, hard_breaking: hard, summary: message,
    compatibility: details.compatibility === "none" || evolution.compatibility === "none" ? "none"
      : typeof evolution.compatibility === "string" ? evolution.compatibility : undefined,
    compatibility_note: typeof evolution.compatibility_note === "string" ? evolution.compatibility_note : undefined,
    schema_evolution: { should_pause: details.should_pause === true || evolution.should_pause === true, hard_breaking: hard },
  };
}

export function SchemaDriftDialog({
  open,
  report,
  onApproveAdditive,
  onReject,
  onRemap,
  onClose,
  onAcknowledge,
}: SchemaDriftDialogProps) {
  if (!open || !report) return null;
  const hard = [
    ...(report.hard_breaking || []), ...(report.schema_evolution?.hard_breaking || []),
    ...(report.breaking || []).filter((change) => !["drop", "rename"].includes(change.kind)),
  ]
    .filter((change, index, changes) => changes.findIndex(
      (candidate) => candidate.kind === change.kind && candidate.column === change.column,
    ) === index);
  const hasHard = hard.length > 0
    || report.schema_evolution?.should_pause === true
    || report.compatibility === "none" || report.schema_evolution?.compatibility === "none";
  const hasAdditive = (report.additive || []).length > 0;
  const compatibilityNote = report.compatibility_note || report.schema_evolution?.compatibility_note;
  const changes = [
    ...(report.additive || []).map((change) => ({
      kind: change.kind, column: change.column, proposed: change.to_type || change.new_type,
      current: change.from_type || change.old_type,
      tone: "added",
    })),
    ...(report.breaking || []).map((change) => ({
      kind: change.kind, column: change.column, proposed: change.to_type || change.new_type || change.to,
      current: change.from_type || change.old_type,
      tone: hard.some((candidate) => candidate.column === change.column && candidate.kind === change.kind)
        ? "blocked" : "changed",
    })),
    ...hard.filter((change) => !(report.breaking || []).some(
      (candidate) => candidate.kind === change.kind && candidate.column === change.column,
    )).map((change) => ({
      kind: change.kind, column: change.column, current: change.from_type || change.old_type,
      proposed: change.to_type || change.new_type || change.to, tone: "blocked",
    })),
  ];

  return (
    <Dialog
      open={open}
      onClose={onClose}
      title="Schema change review"
      subtitle={report.summary || `${changes.length} change${changes.length === 1 ? "" : "s"} detected`}
      size="lg"
      className="df2-drift-dialog"
      footer={(
        <div className="df2-drift-actions">
          <Button variant="ghost" onClick={onClose}>Cancel</Button>
          <div className="df2-drift-decisions">
            {onReject && <Button variant="danger" onClick={onReject}><DtIcon name="x" size={14} /> Reject</Button>}
            {onRemap && <Button variant={hasHard ? "primary" : "secondary"} onClick={onRemap}>
              <DtIcon name="transfer" size={14} /> Open remapping
            </Button>}
          {onAcknowledge && !hasHard ? (
            <Button variant="primary" onClick={onAcknowledge}>
              <DtIcon name="check" size={14} /> Acknowledge for this run
            </Button>
          ) : hasAdditive && !hasHard && !onAcknowledge && onApproveAdditive && (
            <Button variant="primary" onClick={onApproveAdditive}>
              <DtIcon name="check" size={14} /> Approve additive
            </Button>
          )}
          </div>
        </div>
      )}
    >
      <div className={`df2-drift-status ${hasHard ? "is-blocked" : hasAdditive ? "is-additive" : "is-review"}`} role="status">
        <DtIcon name={hasHard ? "shield" : "plus"} size={18} />
        <strong>{hasHard ? "Mapping review required" : hasAdditive
          ? report.breaking?.length ? "Mixed schema changes" : "Additive changes"
          : "Contract review required"}</strong>
        {hasHard && <span>Additive approval unavailable</span>}
      </div>
      {compatibilityNote && <p className="df2-drift-note">{compatibilityNote}</p>}
      {onAcknowledge && <p className="df2-drift-note">Existing mappings retained for this run. Type and key changes remain subject to validation.</p>}
      <dl className="df2-drift-counts">
        <div><dt>Added</dt><dd>{report.additive?.length || 0}</dd></div>
        <div><dt>Changed</dt><dd>{report.breaking?.length || 0}</dd></div>
        <div><dt>Blocking</dt><dd>{hard.length || (hasHard ? 1 : 0)}</dd></div>
      </dl>
      <div className="df2-drift-table-wrap">
        <table className="df2-drift-table">
          <caption className="df2-sr-only">Detected schema changes</caption>
          <thead><tr><th scope="col">Change</th><th scope="col">Field</th><th scope="col">Current</th><th scope="col">Proposed</th></tr></thead>
          <tbody>
            {changes.map((change, index) => (
              <tr key={`${change.kind}:${change.column || ""}:${index}`}>
                <td data-label="Change"><span className={`df2-drift-kind is-${change.tone}`}>
                  <DtIcon name={change.tone === "added" ? "plus" : change.tone === "blocked" ? "shield" : "transfer"} size={13} />
                  {change.kind.replace(/_/g, " ")}
                </span></td>
                <td data-label="Field"><code>{change.column || "-"}</code></td>
                <td data-label="Current"><code>{change.current || "-"}</code></td>
                <td data-label="Proposed"><code>{change.proposed || "-"}</code></td>
              </tr>
            ))}
            {changes.length === 0 && <tr><td colSpan={4}>No field-level changes reported.</td></tr>}
          </tbody>
        </table>
      </div>
    </Dialog>
  );
}

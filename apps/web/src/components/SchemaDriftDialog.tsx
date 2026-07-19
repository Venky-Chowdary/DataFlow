import { Button } from "./ui/Button";
import type { SchemaDriftReport } from "../lib/api";

interface SchemaDriftDialogProps {
  open: boolean;
  report: SchemaDriftReport | null;
  onApproveAdditive: () => void;
  onReject: () => void;
  onRemap: () => void;
  onClose: () => void;
}

/** Approve / Reject / Remap dialog for schema drift (Pinterest-style classify). */
export function SchemaDriftDialog({
  open,
  report,
  onApproveAdditive,
  onReject,
  onRemap,
  onClose,
}: SchemaDriftDialogProps) {
  if (!open || !report) return null;
  const hasBreaking = (report.breaking || []).length > 0;
  const hasAdditive = (report.additive || []).length > 0;

  return (
    <div className="df2-modal-overlay" role="presentation" onClick={onClose}>
      <div
        className="df2-modal df2-drift-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="df2-drift-title"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="df2-modal-head">
          <h2 id="df2-drift-title">Schema drift detected</h2>
          <p className="df2-muted">
            Severity: <strong>{report.severity || (hasBreaking ? "breaking" : "additive")}</strong>
            {report.summary ? ` — ${report.summary}` : ""}
          </p>
          <p className="df2-muted">
            {hasBreaking
              ? "Next: open remapping or Reject — do not approve until types and keys are safe."
              : hasAdditive
                ? "Next: Approve additive to extend the contract, or open remapping if names should change."
                : "Next: Reject to keep the previous contract, or open remapping to redefine fields."}
          </p>
        </header>
        <div className="df2-modal-body">
          {hasAdditive && (
            <section className="df2-drift-section">
              <h3>Additive (safe to approve)</h3>
              <ul>
                {report.additive.map((a, i) => (
                  <li key={`a-${i}`}>
                    {a.kind}
                    {a.column ? `: ${a.column}` : ""}
                    {a.to_type ? ` → ${a.to_type}` : ""}
                  </li>
                ))}
              </ul>
            </section>
          )}
          {hasBreaking && (
            <section className="df2-drift-section is-breaking">
              <h3>Breaking (requires review)</h3>
              <ul>
                {report.breaking.map((b, i) => (
                  <li key={`b-${i}`}>
                    {b.kind}
                    {b.column ? `: ${b.column}` : ""}
                    {b.to ? ` → ${b.to}` : ""}
                    {b.to_type ? ` (${b.to_type})` : ""}
                  </li>
                ))}
              </ul>
            </section>
          )}
        </div>
        <footer className="df2-modal-foot">
          <Button variant="ghost" onClick={onClose}>
            Dismiss
          </Button>
          <Button variant="danger" onClick={onReject}>
            Reject
          </Button>
          <Button variant="secondary" onClick={onRemap}>
            Open remapping
          </Button>
          {hasAdditive && !hasBreaking && (
            <Button variant="primary" onClick={onApproveAdditive}>
              Approve additive
            </Button>
          )}
        </footer>
      </div>
    </div>
  );
}

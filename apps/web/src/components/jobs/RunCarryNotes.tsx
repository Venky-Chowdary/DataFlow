import {
  foreignKeyProblems,
  readForeignKeyCarry,
  readWriterWarnings,
  type ForeignKeyCarryView,
  type ForeignKeyDecisionView,
  type JobEvidenceCarrier,
} from "../../lib/jobEvidence";

function decisionLine(decision: ForeignKeyDecisionView): string {
  const child = decision.destTable || decision.name || "key";
  const parent = decision.referencedTable ? ` → ${decision.referencedTable}` : "";
  const why = decision.reason ? ` — ${decision.reason}` : "";
  return `${child}${parent}: ${decision.status}${why}`;
}

function foreignKeyHeadline(carry: ForeignKeyCarryView): string {
  if (carry.integrityViolations > 0) {
    const n = carry.integrityViolations;
    return `${n} foreign key${n === 1 ? "" : "s"} rejected — loaded rows do not satisfy the constraint`;
  }
  if (carry.error) return carry.error;
  if (carry.verdict === "carried") {
    const n = carry.carried;
    return `${n} foreign key${n === 1 ? "" : "s"} recreated on the destination`;
  }
  if (carry.verdict === "partial") return "Foreign keys partially carried";
  if (carry.verdict === "unknown") return "Foreign key carry did not finish";
  return carry.verdict || "Foreign keys";
}

/**
 * Writer warnings and foreign-key carry. Both are stamped on
 * destination_summary; this is the only place that lists them.
 */
export function RunCarryNotes({
  job,
  hideCycle = false,
}: {
  job: JobEvidenceCarrier | null | undefined;
  /** Theater already has the cycle sentence on its metric card. */
  hideCycle?: boolean;
}) {
  const warnings = readWriterWarnings(job);
  const carry = readForeignKeyCarry(job);
  const problems = carry ? foreignKeyProblems(carry) : [];
  const showCarry = Boolean(carry) && (!hideCycle || problems.length > 0 || Boolean(carry?.error));
  if (warnings.messages.length === 0 && warnings.suppressed === 0 && !showCarry) return null;

  return (
    <>
      {(warnings.messages.length > 0 || warnings.suppressed > 0) && (
        <section className="df2-result-warnings-block" role="status" aria-label="Writer warnings">
          <p className="df2-result-warnings-note">
            {warnings.messages.length} writer message{warnings.messages.length === 1 ? "" : "s"}
            {warnings.suppressed > 0
              ? ` · ${warnings.suppressed.toLocaleString()} more not listed`
              : ""}
          </p>
          {warnings.messages.length > 0 && (
            <ul className="df2-result-warnings">
              {warnings.messages.map((message, index) => <li key={`${index}-${message}`}>{message}</li>)}
            </ul>
          )}
        </section>
      )}
      {showCarry && carry && (
        <section
          className={
            problems.length > 0 || carry.integrityViolations > 0 || carry.verdict !== "carried" || carry.error
              ? "df2-result-warnings-block"
              : "df2-jobs-overview-note"
          }
          role="status"
          aria-label="Foreign key carry"
        >
          <p className={problems.length > 0 || carry.verdict !== "carried" ? "df2-result-warnings-note" : undefined}>
            {foreignKeyHeadline(carry)}
          </p>
          {!hideCycle && carry.cycle.length > 0 && (
            <p className="df2-result-warnings-note">
              {carry.cycleResolved
                ? `Cycle ${carry.cycle.join(", ")} recreated after the load`
                : carry.cycleNote || `Cycle ${carry.cycle.join(", ")} is not fully enforced`}
            </p>
          )}
          {problems.length > 0 && (
            <ul className="df2-result-warnings">
              {problems.map((decision, index) => (
                <li key={`${decision.destTable}-${decision.name}-${index}`}>{decisionLine(decision)}</li>
              ))}
            </ul>
          )}
        </section>
      )}
    </>
  );
}

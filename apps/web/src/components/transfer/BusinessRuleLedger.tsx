import type { CompiledRule, RuleCompileReport } from "../../lib/businessRules";
import { ruleReportSummary, ruleStatusLabel } from "../../lib/businessRules";

interface BusinessRuleLedgerProps {
  report: RuleCompileReport;
  /** When true, start expanded so review rows are visible. */
  defaultOpen?: boolean;
}

function lineClass(status: string): string {
  if (status === "executable") return "is-applied";
  if (status === "conflict") return "is-conflict";
  return "is-review";
}

function provenance(rule: CompiledRule): string {
  const sheet = rule.provenance?.sheet;
  const row = rule.provenance?.row;
  if (sheet && row) return `${sheet} · row ${row}`;
  if (row) return `row ${row}`;
  if (sheet) return sheet;
  return "";
}

/**
 * Line-by-line compiled workbook. Every uploaded row is listed — applied,
 * review, or conflict — so the operator can read the file the compiler read.
 */
export function BusinessRuleLedger({
  report,
  defaultOpen,
}: BusinessRuleLedgerProps) {
  const open = defaultOpen
    ?? (report.buckets.needs_confirmation > 0 || report.buckets.conflict > 0);
  return (
    <details className="df2-rule-ledger" open={open}>
      <summary>
        <strong>Rules, line by line</strong>
        <span>{ruleReportSummary(report)}</span>
      </summary>
      <ol className="df2-rule-ledger-list">
        {report.rules.map((rule, index) => (
          <li
            key={`${rule.provenance?.sheet || "sheet"}-${rule.provenance?.row || index}-${rule.source_column}-${rule.dest_column}`}
            className={`df2-rule-line ${lineClass(rule.status)}`}
          >
            <span className="df2-rule-line-meta">{provenance(rule) || `line ${index + 1}`}</span>
            <span className="df2-rule-line-edge">
              {rule.source_column || "—"}
              <span aria-hidden> → </span>
              {rule.dest_column || "—"}
            </span>
            <span className="df2-rule-line-text" title={rule.rule_text}>
              {rule.rule_text || "(direct)"}
            </span>
            <span className={`df2-badge df2-badge-xs df2-rule-chip ${lineClass(rule.status)}`}>
              {rule.kind_label}
            </span>
            <span className="df2-rule-line-status">{ruleStatusLabel(rule.status)}</span>
            {rule.issues?.length ? (
              <span className="df2-rule-line-issue">{rule.issues[0]}</span>
            ) : null}
          </li>
        ))}
      </ol>
      {report.unused_dest_count > 0 ? (
        <p className="df2-rule-ledger-unused">
          {report.unused_dest_count} destination column
          {report.unused_dest_count === 1 ? "" : "s"} unused and not written
          {report.unused_dest_columns.length
            ? `: ${report.unused_dest_columns.slice(0, 12).join(", ")}${
              report.unused_dest_count > 12 ? ` +${report.unused_dest_count - 12} more` : ""
            }`
            : "."}
        </p>
      ) : null}
    </details>
  );
}

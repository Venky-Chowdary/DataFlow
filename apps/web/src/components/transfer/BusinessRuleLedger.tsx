import type { CompiledRule, RuleCompileReport } from "../../lib/businessRules";
import {
  canAcceptAsDirect,
  ruleActionLabel,
  ruleConfidenceLabel,
  ruleReportSummary,
  ruleStatusLabel,
} from "../../lib/businessRules";

interface BusinessRuleLedgerProps {
  report: RuleCompileReport;
  /** When true, start expanded so review rows are visible. */
  defaultOpen?: boolean;
  /** Operator accept of a bound rename — never invents a transform. */
  onAcceptDirect?: (index: number) => void;
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

function edgeLabel(rule: CompiledRule): string {
  if (rule.kind === "contract") {
    const col = rule.dest_column || rule.source_column || "—";
    const name = rule.named_rule ? ` · ${rule.named_rule}` : "";
    return `${col}${name}`;
  }
  return `${rule.source_column || "—"} → ${rule.dest_column || "—"}`;
}

function reviewHint(rule: CompiledRule): string {
  if (rule.kind === "contract" && rule.status === "executable") {
    return "Destination validation — compiled IR, not a Transform write.";
  }
  if (rule.kind === "contract") {
    return "Validate contract — confirm the column binding. This is not a Transform write.";
  }
  if (rule.kind === "join") {
    return "Joins stay in review — this compiler will not invent a grain.";
  }
  if (canAcceptAsDirect(rule)) {
    return "Clear rename / passthrough the compiler left for you. Accept as Direct to put it on Map, or leave it and remap there.";
  }
  return "The compiler will not invent an algorithm from this sentence. Confirm or remap on Map.";
}

/**
 * Line-by-line compiled workbook. Every uploaded row is listed — applied,
 * review, or conflict — so the operator can read the file the compiler read.
 */
export function BusinessRuleLedger({
  report,
  defaultOpen,
  onAcceptDirect,
}: BusinessRuleLedgerProps) {
  const open = defaultOpen
    ?? (report.buckets.needs_confirmation > 0 || report.buckets.conflict > 0);
  return (
    <details className="df2-rule-ledger" open={open}>
      <summary>
        <strong>Rule analysis</strong>
        <span>{ruleReportSummary(report)}</span>
      </summary>
      <p className="df2-rule-ledger-how">
        Closed-form rows compile to structured IR, then Transform / Map /
        Validate execute them. Review is required when the sentence is not
        a closed form or the column did not bind. Validate contracts never
        write a destination column. Accept a leftover bound rename as Direct
        here — Map is where you remap the rest.
      </p>
      {report.sheet_kinds?.length ? (
        <p className="df2-rule-ledger-unused" aria-label="Workbook sheets">
          {report.sheet_kinds.map((item) => `${item.sheet || "sheet"}: ${item.kind} (${item.rows})`).join(" · ")}
          {report.matcher ? ` · matcher ${report.matcher}` : ""}
          {report.lookup_coverage?.length
            ? ` · ${report.lookup_coverage.reduce((sum, item) => sum + item.pairs, 0)} lookup pair(s)`
            : ""}
          {report.named_rules?.length
            ? ` · ${report.named_rules.length} named rule(s)`
            : ""}
        </p>
      ) : null}
      {report.header_roles?.length ? (
        <ul className="df2-rule-ledger-roles" aria-label="How this file was read">
          {report.header_roles.map((item) => (
            <li key={`${item.sheet || ""}-${item.header}-${item.role}`}>
              <strong>{item.header}</strong>
              <span> → {item.role.replace(/_/g, " ")}</span>
              {item.reason ? <span> · {item.reason}</span> : null}
            </li>
          ))}
        </ul>
      ) : null}
      <div className="df2-rule-analysis-head">
        <span>Line</span>
        <span>Rule</span>
        <span>Interpretation</span>
        <span>Confidence</span>
        <span>Action</span>
      </div>
      <ol className="df2-rule-ledger-list">
        {report.rules.map((rule, index) => (
          <li
            key={`${rule.provenance?.sheet || "sheet"}-${rule.provenance?.row || index}-${rule.source_column}-${rule.dest_column}`}
            className={`df2-rule-line ${lineClass(rule.status)}`}
          >
            <span className="df2-rule-line-meta">{provenance(rule) || `line ${index + 1}`}</span>
            <span className="df2-rule-line-edge">{edgeLabel(rule)}</span>
            <span className="df2-rule-line-read">
              {rule.interpretation || rule.kind_label}
            </span>
            <span className="df2-rule-line-confidence">{ruleConfidenceLabel(rule.confidence)}</span>
            <span className={`df2-rule-line-action ${lineClass(rule.status)}`}>
              {ruleActionLabel(rule.action, rule.status)}
            </span>
            <span className="df2-rule-line-text" title={rule.resolved_rule || rule.rule_text}>
              {rule.named_rule && rule.kind !== "contract"
                ? `${rule.rule_text || rule.named_rule} → ${rule.resolved_rule || rule.named_rule}`
                : (rule.rule_text || "(direct)")}
            </span>
            <span className={`df2-badge df2-badge-xs df2-rule-chip ${lineClass(rule.status)}`}>
              {rule.kind_label}
            </span>
            <span className="df2-rule-line-status">{ruleStatusLabel(rule.status)}</span>
            {rule.unknown_code_policy?.action && rule.unknown_code_policy.action !== "refuse" ? (
              <span className="df2-rule-line-issue">
                unknown codes {rule.unknown_code_policy.action}
                {rule.unknown_code_policy.value ? ` → ${rule.unknown_code_policy.value}` : ""}
                {" "}(G20 still refuses)
              </span>
            ) : null}
            {rule.issues?.length ? (
              <span className="df2-rule-line-issue">{rule.issues.join(" · ")}</span>
            ) : null}
            {rule.status === "needs_confirmation" ? (
              <span className="df2-rule-line-next">
                <span>{reviewHint(rule)}</span>
                {onAcceptDirect && canAcceptAsDirect(rule) ? (
                  <button
                    type="button"
                    className="df2-btn df2-btn-sm"
                    onClick={() => onAcceptDirect(index)}
                  >
                    Accept as Direct
                  </button>
                ) : null}
              </span>
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
      {(report.unmapped_source_count ?? 0) > 0 ? (
        <p className="df2-rule-ledger-unused">
          {report.unmapped_source_count} source column
          {report.unmapped_source_count === 1 ? "" : "s"} not named in the workbook
          {report.unmapped_source_columns?.length
            ? `: ${report.unmapped_source_columns.slice(0, 12).join(", ")} — remap or omit, never silent drop`
            : " — remap or omit, never silent drop"}
        </p>
      ) : null}
    </details>
  );
}

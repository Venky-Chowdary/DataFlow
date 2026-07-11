import { DtIcon } from "../DtIcon";
import { detectTypeRisks, type TypeRisk } from "../../lib/schemaIntelligence";
import type { EditableMapping } from "../../lib/mapping";
import type { EnhancedAnalysis, PreflightResult, TransferResult } from "../../lib/types";

interface TransferStudioInspectorProps {
  step: number;
  analysis?: EnhancedAnalysis | null;
  columnMappings: EditableMapping[];
  preflight?: PreflightResult | null;
  result?: TransferResult | null;
  onGoToMapping?: () => void;
}

const STEP_GUIDES: Record<number, { title: string; body: string }> = {
  1: {
    title: "Source",
    body: "Upload a file or connect a database. Schema preview appears on the right as soon as data is profiled.",
  },
  2: {
    title: "Destination",
    body: "Pick connector, database, and table. Existing destination schema is fetched before mapping.",
  },
  3: {
    title: "Map",
    body: "Intelligent mapping aligns source columns to destination fields. Review critical and PII fields.",
  },
  4: {
    title: "Validate",
    body: "Preflight runs eleven gates — transforms, destination probe, and capacity checks.",
  },
  5: {
    title: "Run",
    body: "Live batch progress with phase tracking. Data appends to existing tables by default.",
  },
};

function riskIcon(severity: TypeRisk["severity"]) {
  if (severity === "block") return "alert";
  if (severity === "warn") return "shield";
  return "sparkle";
}

/** Context rail — step guide + issues only (no score rings). */
export function TransferStudioInspector({
  step,
  analysis,
  columnMappings,
  preflight,
  result,
  onGoToMapping,
}: TransferStudioInspectorProps) {
  const typeRisks = detectTypeRisks(columnMappings, analysis, null);
  const blockers = typeRisks.filter((r) => r.severity === "block");
  const warnings = typeRisks.filter((r) => r.severity === "warn");

  const showRisks = step >= 3 && step <= 4 && typeRisks.length > 0;
  const showPreflight = step >= 4 && preflight;
  const showResult = step === 5 && result?.success;
  const guide = STEP_GUIDES[step] ?? STEP_GUIDES[1];

  return (
    <aside className="df2-studio-inspector" aria-label="Step context">
      <div className="df2-inspector-panel df2-inspector-guide">
        <strong>{guide.title}</strong>
        <p>{guide.body}</p>
      </div>

      {showRisks && (
        <div className="df2-inspector-panel">
          <div className="df2-inspector-kicker">
            Needs attention
            {blockers.length > 0 && (
              <span className="df2-badge df2-badge-error df2-badge-xs">{blockers.length}</span>
            )}
          </div>
          <ul className="df2-inspector-risks">
            {[...blockers, ...warnings].slice(0, 5).map((risk) => (
              <li key={risk.id} className={`df2-inspector-risk ${risk.severity}`}>
                <DtIcon name={riskIcon(risk.severity)} size={13} />
                <div>
                  <strong>{risk.column}</strong>
                  <span>{risk.title}</span>
                </div>
              </li>
            ))}
          </ul>
          {blockers.length > 0 && onGoToMapping && step !== 2 && (
            <button type="button" className="df2-btn df2-btn-sm df2-inspector-action" onClick={onGoToMapping}>
              Review mappings
            </button>
          )}
        </div>
      )}

      {showPreflight && (
        <div className="df2-inspector-panel">
          <div className="df2-inspector-kicker">Preflight</div>
          <p className="df2-inspector-preflight-line">
            <strong>{preflight.passed_count}/{preflight.total_gates}</strong> checks passed
            {!preflight.passed && " — fix blockers before running"}
          </p>
        </div>
      )}

      {showResult && (
        <div className="df2-inspector-panel df2-inspector-success">
          <DtIcon name="check" size={16} />
          <div>
            <strong>{result.records_transferred?.toLocaleString()} rows transferred</strong>
            {result.reconciliation?.message && <span>{result.reconciliation.message}</span>}
          </div>
        </div>
      )}
    </aside>
  );
}

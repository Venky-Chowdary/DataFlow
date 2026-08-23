import { DtIcon } from "../DtIcon";
import { detectTypeRisks, type TypeRisk } from "../../lib/schemaIntelligence";
import type { EditableMapping } from "../../lib/mapping";
import type { EnhancedAnalysis, PreflightResult, TransferResult } from "../../lib/types";
import { conservationCompleteCopy } from "../../lib/conservationLedger";
import {
  STEP_DESTINATION,
  STEP_MAP,
  STEP_RUN,
  STEP_SHAPE,
  STEP_SOURCE,
  STEP_VALIDATE,
} from "../../pages/transfer/studioConstants";

interface TransferStudioInspectorProps {
  step: number;
  analysis?: EnhancedAnalysis | null;
  columnMappings: EditableMapping[];
  preflight?: PreflightResult | null;
  result?: TransferResult | null;
  /** Destination connector id — enables SaaS width/scale fidelity chips. */
  destType?: string;
  onGoToMapping?: () => void;
}

function resultDestLabel(result: TransferResult): string {
  return conservationCompleteCopy(result);
}

const STEP_GUIDES: Record<number, { title: string; body: string }> = {
  [STEP_SOURCE]: {
    title: "Source",
    body: "Upload a file or connect a database. Schema preview appears on the right as soon as data is profiled.",
  },
  [STEP_DESTINATION]: {
    title: "Destination",
    body: "Pick connector, database, and table. Existing destination schema is fetched before mapping.",
  },
  [STEP_SHAPE]: {
    title: "Shape",
    body: "Optional. Clean the source on the read — trim, parse, round, filter — before Map decides carriers. The source itself is never modified, and the recipe is re-applied identically at Execute.",
  },
  [STEP_MAP]: {
    title: "Map",
    body: "Intelligent mapping aligns source columns to destination fields. Review critical and PII fields.",
  },
  [STEP_VALIDATE]: {
    title: "Validate",
    body: "Preflight runs core G1–G9 only (not “eleven gates”). Studio may add host policy checks and soft constraint hints — those are extras, not marketed GateIds.",

  },
  [STEP_RUN]: {
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
  destType,
  onGoToMapping,
}: TransferStudioInspectorProps) {
  const typeRisks = detectTypeRisks(columnMappings, analysis, null, {
    destConnector: destType,
  });
  const blockers = typeRisks.filter((r) => r.severity === "block");
  const warnings = typeRisks.filter((r) => r.severity === "warn");

  const showRisks = step >= STEP_MAP && step <= STEP_VALIDATE && typeRisks.length > 0;
  const showPreflight = step >= STEP_VALIDATE && preflight;
  const showResult = step === STEP_RUN && result?.success;
  const guide = STEP_GUIDES[step] ?? STEP_GUIDES[STEP_SOURCE];

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
          {blockers.length > 0 && onGoToMapping && step !== STEP_DESTINATION && (
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
            <strong>{resultDestLabel(result)}</strong>
            {result.reconciliation?.message && <span>{result.reconciliation.message}</span>}
          </div>
        </div>
      )}
    </aside>
  );
}

import { DtIcon } from "./DtIcon";
import {
  buildCompetitiveAdvantages,
  detectNestedDocumentFields,
  detectTypeRisks,
  inferSourceFormatLabel,
  intelligenceScore,
  summarizeColumns,
  type TypeRisk,
} from "../lib/schemaIntelligence";
import type { EditableMapping } from "../lib/mapping";
import type { EnhancedAnalysis, PreflightResult, TransferPlan, TransferResult } from "../lib/types";

interface SchemaIntelligenceRailProps {
  step: number;
  analysis?: EnhancedAnalysis | null;
  columnMappings: EditableMapping[];
  transferPlan?: TransferPlan | null;
  preflight?: PreflightResult | null;
  result?: TransferResult | null;
  destType?: string;
  sourceKind?: string;
  sourceFormat?: string;
  rowCount?: number;
  sampleRows?: Record<string, unknown>[];
  syncModeLabel?: string;
  schemaPolicyLabel?: string;
  validationMode?: string;
  onGoToMapping?: () => void;
  onRunPreflight?: () => void;
}

function riskIcon(severity: TypeRisk["severity"]) {
  if (severity === "block") return "alert";
  if (severity === "warn") return "shield";
  return "sparkle";
}

export function SchemaIntelligenceRail({
  step,
  analysis,
  columnMappings,
  transferPlan,
  preflight,
  result,
  destType,
  sourceKind = "file",
  sourceFormat,
  rowCount,
  sampleRows,
  syncModeLabel,
  schemaPolicyLabel,
  validationMode,
  onGoToMapping,
  onRunPreflight,
}: SchemaIntelligenceRailProps) {
  const columns = analysis?.columns.map((c) => c.column_name) ?? columnMappings.map((m) => m.source);
  const nestedFields = detectNestedDocumentFields(columns, sampleRows);
  const typeRisks = detectTypeRisks(columnMappings, analysis, transferPlan);
  const score = intelligenceScore(analysis, preflight, typeRisks);
  const colSummary = summarizeColumns(analysis);
  const blockers = typeRisks.filter((r) => r.severity === "block");
  const warnings = typeRisks.filter((r) => r.severity === "warn");
  const formatLabel = inferSourceFormatLabel(analysis, sourceFormat);
  const advantages = buildCompetitiveAdvantages({
    sourceKind,
    destType,
    columnCount: colSummary.total,
    hasPreflight: Boolean(preflight),
    hasCrossDb: Boolean(destType && destType !== "mongodb"),
    nestedFieldCount: nestedFields.length,
  });

  return (
    <aside className="df2-intelligence-rail" aria-label="Schema intelligence">
      <div className="df2-rail-panel df2-intelligence-hero">
        <div className="df2-rail-head">
          <DtIcon name="sparkle" size={18} />
          <div>
            <h3>Schema intelligence</h3>
            <p>Governed migration — beyond raw JSON import</p>
          </div>
        </div>
        <div className="df2-intelligence-score">
          <strong>{score > 0 ? `${score}%` : "—"}</strong>
          <span>{preflight ? "readiness" : analysis ? "mapping quality" : "awaiting source"}</span>
        </div>
        {rowCount != null && rowCount > 0 && (
          <p className="df2-intelligence-meta">{rowCount.toLocaleString()} rows · {formatLabel}</p>
        )}
      </div>

      <div className="df2-rail-panel df2-intelligence-advantages">
        <div className="df2-rail-kicker">Why DataFlow</div>
        <ul className="df2-advantage-list">
          {advantages.map((adv) => (
            <li key={adv.id}>
              <DtIcon name={adv.icon} size={14} />
              <div>
                <strong>{adv.title}</strong>
                <span>{adv.detail}</span>
              </div>
            </li>
          ))}
        </ul>
      </div>

      {step === 1 && !analysis && (
        <div className="df2-rail-panel">
          <p className="df2-intelligence-empty">
            Upload JSON/CSV, connect MongoDB, or point at S3 — we profile schema, detect nested documents,
            map to Snowflake/Postgres/BigQuery with type safety. Compass imports BSON locally; DataFlow governs the full route.
          </p>
        </div>
      )}

      {analysis && (
        <div className="df2-rail-panel">
          <div className="df2-rail-kicker">Profiled schema</div>
          <div className="df2-intelligence-stats">
            <div><strong>{colSummary.total}</strong><span>fields</span></div>
            <div><strong>{colSummary.highConfidence}</strong><span>high match</span></div>
            <div><strong>{colSummary.pii}</strong><span>PII</span></div>
            <div><strong>{colSummary.lowConfidence}</strong><span>review</span></div>
          </div>
          {analysis.recommendations.slice(0, 2).map((rec) => (
            <p key={rec} className="df2-rail-note">{rec}</p>
          ))}
        </div>
      )}

      {nestedFields.length > 0 && (
        <div className="df2-rail-panel">
          <div className="df2-rail-kicker">Nested / JSON fields</div>
          <ul className="df2-nested-field-list">
            {nestedFields.map((nf) => (
              <li key={nf.column}>
                <code>{nf.column}</code>
                <span>{nf.detail}</span>
                {nf.flattenTarget && (
                  <small>→ {nf.flattenTarget}</small>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}

      {transferPlan && transferPlan.type_mappings.length > 0 && (
        <div className="df2-rail-panel">
          <div className="df2-rail-kicker">Type contract → {destType ?? "destination"}</div>
          <div className="df2-type-mapping-list">
            {transferPlan.type_mappings.slice(0, 6).map((tm) => (
              <div key={tm.column} className="df2-type-mapping-row">
                <code>{tm.column}</code>
                <span>{tm.source_type} → {tm.dest_type}</span>
              </div>
            ))}
            {transferPlan.type_mappings.length > 6 && (
              <p className="df2-rail-note">+{transferPlan.type_mappings.length - 6} more native DDL mappings</p>
            )}
          </div>
        </div>
      )}

      {typeRisks.length > 0 && (
        <div className="df2-rail-panel">
          <div className="df2-rail-kicker">
            Type safety {blockers.length > 0 && <span className="df2-badge df2-badge-error df2-badge-xs">{blockers.length} block</span>}
          </div>
          <ul className="df2-type-risk-list">
            {typeRisks.map((risk) => (
              <li key={risk.id} className={`df2-type-risk ${risk.severity}`}>
                <DtIcon name={riskIcon(risk.severity)} size={14} />
                <div>
                  <strong>{risk.column}</strong>
                  <span>{risk.title}</span>
                  <small>{risk.detail}</small>
                </div>
              </li>
            ))}
          </ul>
          {(blockers.length > 0 || warnings.length > 0) && onGoToMapping && step >= 2 && (
            <button type="button" className="df2-btn df2-btn-sm df2-rail-action-btn" onClick={onGoToMapping}>
              <DtIcon name="sparkle" size={14} /> Fix in mapping
            </button>
          )}
        </div>
      )}

      {preflight && (
        <div className="df2-rail-panel">
          <div className="df2-rail-kicker">Preflight gates</div>
          <div className="df2-rail-split">
            <span>Passed</span>
            <strong>{preflight.passed_count}/{preflight.total_gates}</strong>
          </div>
          {preflight.gates.slice(0, 5).map((g) => (
            <div key={g.id} className={`df2-gate-row ${g.status}`}>
              <DtIcon name={g.status === "pass" ? "check" : g.status === "block" ? "x" : "activity"} size={12} />
              <span>{g.message}</span>
            </div>
          ))}
          {!preflight.passed && preflight.blockers.length > 0 && onRunPreflight && (
            <button type="button" className="df2-btn df2-btn-primary df2-btn-sm df2-rail-action-btn" onClick={onRunPreflight}>
              Re-run preflight
            </button>
          )}
        </div>
      )}

      {(syncModeLabel || schemaPolicyLabel) && step >= 3 && (
        <div className="df2-rail-panel">
          <div className="df2-rail-kicker">Run settings</div>
          {syncModeLabel && (
            <div className="df2-rail-split"><span>Sync</span><strong>{syncModeLabel}</strong></div>
          )}
          {schemaPolicyLabel && (
            <div className="df2-rail-split"><span>Schema</span><strong>{schemaPolicyLabel}</strong></div>
          )}
          {validationMode && (
            <div className="df2-rail-split"><span>Validation</span><strong>{validationMode}</strong></div>
          )}
        </div>
      )}

      {result?.success && (
        <div className="df2-rail-panel">
          <div className="df2-rail-kicker">Reconciliation</div>
          <div className="df2-rail-split">
            <span>Rows written</span>
            <strong>{result.records_transferred?.toLocaleString() ?? "0"}</strong>
          </div>
          {result.reconciliation?.message && (
            <p className="df2-rail-note">{result.reconciliation.message}</p>
          )}
        </div>
      )}
    </aside>
  );
}

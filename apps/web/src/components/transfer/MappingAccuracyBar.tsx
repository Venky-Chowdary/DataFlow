import { DtIcon } from "../DtIcon";
import { mappingRequiresRiskAck, type EditableMapping } from "../../lib/mapping";
import { isCreateNewColumn } from "../../lib/columnWorkbench";
import { ProgressRing } from "../ui/ProgressRing";

interface MappingAccuracyBarProps {
  mappings: EditableMapping[];
  confidenceThreshold: number;
  llmUsed?: boolean;
}

export function MappingAccuracyBar({
  mappings,
  confidenceThreshold: _confidenceThreshold,
  llmUsed,
}: MappingAccuracyBarProps) {
  const total = mappings.length;
  const lossy = (m: EditableMapping) => mappingRequiresRiskAck(m) && !m.riskAcknowledged;
  // Ready ≡ operator-approved only — confidence never invents green.
  const ready = mappings.filter((m) => !lossy(m) && m.approved).length;
  const review = mappings.filter(
    (m) => !m.approved || lossy(m),
  ).length;
  const riskOpen = mappings.filter(lossy).length;
  const pii = mappings.filter((m) => m.isPii).length;
  const avgConf = total
    ? Math.round((mappings.reduce((s, m) => s + m.confidence, 0) / total) * 100)
    : 0;
  const matchPct = total ? Math.round((ready / total) * 100) : 0;
  const existsInDest = mappings.filter((m) => m.existsInDestination).length;
  const newFields = mappings.filter((m) => isCreateNewColumn(m)).length;

  return (
    <div className="df2-mapping-accuracy" role="status" aria-label="Mapping approval coverage — not Execute clearance">
      <ProgressRing value={matchPct} size={56} tone={matchPct >= 100 ? "ok" : review > 0 ? "warn" : "ok"}>
        <strong>{matchPct}%</strong>
        <small>approved</small>
      </ProgressRing>

      <div className="df2-mapping-accuracy-stats">
        <div className="df2-mapping-accuracy-stat ok">
          <DtIcon name="check" size={14} />
          <span><strong>{ready}</strong> approved</span>
        </div>
        {riskOpen > 0 && (
          <div className="df2-mapping-accuracy-stat block">
            <DtIcon name="alert" size={14} />
            <span><strong>{riskOpen}</strong> need Accept risk</span>
          </div>
        )}
        <div className="df2-mapping-accuracy-stat">
          <DtIcon name="sparkle" size={14} />
          <span><strong>{avgConf}%</strong> avg confidence</span>
        </div>
        {review > 0 && (
          <div className="df2-mapping-accuracy-stat warn">
            <DtIcon name="alert" size={14} />
            <span><strong>{review}</strong> review</span>
          </div>
        )}
        {pii > 0 && (
          <div className="df2-mapping-accuracy-stat block">
            <DtIcon name="shield" size={14} />
            <span><strong>{pii}</strong> PII</span>
          </div>
        )}
        {existsInDest > 0 && (
          <div className="df2-mapping-accuracy-stat">
            <span><strong>{existsInDest}</strong> exist in dest</span>
          </div>
        )}
        {newFields > 0 && (
          <div className="df2-mapping-accuracy-stat">
            <span><strong>{newFields}</strong> new fields</span>
          </div>
        )}
        {llmUsed && (
          <span className="df2-badge df2-badge-live df2-badge-xs">
            <DtIcon name="sparkle" size={10} /> AI mapped
          </span>
        )}
      </div>
    </div>
  );
}

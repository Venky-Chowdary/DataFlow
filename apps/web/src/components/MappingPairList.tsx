import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import type { IndexedMapping } from "../lib/columnWorkbench";
import { mappingTier } from "../lib/columnWorkbench";
import { fidelityChipLabel, fidelityRiskForMapping } from "../lib/schemaIntelligence";

interface MappingPairListProps {
  items: IndexedMapping[];
  sourceLabel: string;
  sourceSubtitle?: string;
  sourceType: string;
  destLabel: string;
  destSubtitle?: string;
  destType: string;
  confidenceThreshold?: number;
  totalCount?: number;
  onSelectSource?: (source: string) => void;
}

/** Compact source → destination pair list for wide schemas (replaces dense SVG canvas). */
export function MappingPairList({
  items,
  sourceLabel,
  sourceSubtitle,
  sourceType,
  destLabel,
  destSubtitle,
  destType,
  confidenceThreshold = 0.85,
  totalCount,
  onSelectSource,
}: MappingPairListProps) {
  const showing = items.length;
  const total = totalCount ?? showing;

  return (
    <div className="df2-mapping-pairs" aria-label="Column mapping pairs">
      <div className="df2-mapping-pairs-route">
        <div className="df2-mapping-pairs-endpoint">
          <ConnectorIcon id={sourceType} size={18} />
          <div className="df2-mapping-pairs-endpoint-text">
            <span className="df2-mapping-pairs-kind">Source</span>
            <strong title={sourceLabel}>{sourceLabel}</strong>
            <small title={sourceSubtitle || undefined}>{sourceSubtitle || "\u00A0"}</small>
          </div>
        </div>
        <div className="df2-mapping-pairs-bridge" aria-hidden>
          <DtIcon name="transfer" size={14} />
        </div>
        <div className="df2-mapping-pairs-endpoint">
          <ConnectorIcon id={destType} size={18} />
          <div className="df2-mapping-pairs-endpoint-text">
            <span className="df2-mapping-pairs-kind">Destination</span>
            <strong title={destLabel}>{destLabel}</strong>
            <small title={destSubtitle || undefined}>{destSubtitle || "\u00A0"}</small>
          </div>
        </div>
      </div>

      <div className="df2-mapping-pairs-meta">
        Showing {showing.toLocaleString()}
        {total !== showing ? ` of ${total.toLocaleString()}` : ""} mapped fields
      </div>

      <div className="df2-mapping-pairs-head" aria-hidden>
        <span>Source field</span>
        <span aria-hidden>→</span>
        <span>Destination field</span>
        <span>Match</span>
      </div>

      <ul className="df2-mapping-pairs-list">
        {items.map(({ mapping, index }) => {
          const tier = mappingTier(mapping, confidenceThreshold);
          // Prefer the engine's stamped verdict over client-side heuristics so
          // Map, the proof drawer, and Pilot all show the same risk chip.
          const engineVerdict = (mapping.fidelity || "").toLowerCase();
          const engineRisk =
            engineVerdict === "lossy_cast" || engineVerdict === "mutate" || engineVerdict === "cast"
              ? {
                  label: engineVerdict === "lossy_cast" ? "lossy" : engineVerdict === "mutate" ? "mutate" : "cast",
                  detail: mapping.fidelityReason || `${mapping.inferredType || "?"} → ${mapping.destType || "?"}`,
                  severity: engineVerdict === "lossy_cast" ? "block" as const : "warn" as const,
                }
              : null;
          const heuristic = engineRisk
            ? null
            : fidelityRiskForMapping(mapping, { destConnector: destType });
          const fidelityLabel = engineRisk
            ? engineRisk.label
            : heuristic
              ? fidelityChipLabel(heuristic)
              : null;
          const fidelityDetail = engineRisk?.detail || heuristic?.detail || "";
          const fidelitySeverity = engineRisk?.severity || heuristic?.severity || "warn";
          const hasFidelity = Boolean(fidelityLabel);
          const pairTitle = [
            `${mapping.source} → ${mapping.target} (${(mapping.confidence * 100).toFixed(0)}%)`,
            fidelityLabel ? `${fidelityLabel}: ${fidelityDetail}` : "",
          ].filter(Boolean).join(" · ");
          return (
            <li key={`${mapping.source}-${index}`}>
              <button
                type="button"
                className={`df2-mapping-pair df2-mapping-pair-${tier}${hasFidelity ? " has-fidelity-risk" : ""}`}
                onClick={() => onSelectSource?.(mapping.source)}
                title={pairTitle}
              >
                <span className="df2-mapping-pair-source">{mapping.source}</span>
                <span className="df2-mapping-pair-arrow" aria-hidden>→</span>
                <span className="df2-mapping-pair-target">{mapping.target}</span>
                <span className={`df2-mapping-pair-conf df2-mapping-pair-conf-${tier}`}>
                  {(mapping.confidence * 100).toFixed(0)}%
                </span>
                {fidelityLabel && (
                  <span
                    className={`df2-badge df2-badge-xs ${fidelitySeverity === "block" ? "df2-badge-run" : "df2-badge-warn"}`}
                    title={fidelityDetail}
                  >
                    {fidelityLabel}
                  </span>
                )}
                {mapping.isPii && (
                  <span className="df2-badge df2-badge-run df2-badge-xs">PII</span>
                )}
                {mapping.requiresReview && !mapping.approved && !hasFidelity && (
                  <span className="df2-badge df2-badge-run df2-badge-xs">review</span>
                )}
              </button>
            </li>
          );
        })}
        {items.length === 0 && (
          <li className="df2-mapping-pairs-empty">No columns match the current search or filter.</li>
        )}
      </ul>
    </div>
  );
}

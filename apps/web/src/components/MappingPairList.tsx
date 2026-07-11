import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import type { IndexedMapping } from "../lib/columnWorkbench";
import { mappingTier } from "../lib/columnWorkbench";

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
          <div>
            <span className="df2-mapping-pairs-kind">Source</span>
            <strong title={sourceLabel}>{sourceLabel}</strong>
            {sourceSubtitle && <small>{sourceSubtitle}</small>}
          </div>
        </div>
        <div className="df2-mapping-pairs-bridge" aria-hidden>
          <DtIcon name="transfer" size={14} />
        </div>
        <div className="df2-mapping-pairs-endpoint">
          <ConnectorIcon id={destType} size={18} />
          <div>
            <span className="df2-mapping-pairs-kind">Destination</span>
            <strong title={destLabel}>{destLabel}</strong>
            {destSubtitle && <small>{destSubtitle}</small>}
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
          return (
            <li key={`${mapping.source}-${index}`}>
              <button
                type="button"
                className={`df2-mapping-pair df2-mapping-pair-${tier}`}
                onClick={() => onSelectSource?.(mapping.source)}
                title={`${mapping.source} → ${mapping.target} (${(mapping.confidence * 100).toFixed(0)}%)`}
              >
                <span className="df2-mapping-pair-source">{mapping.source}</span>
                <span className="df2-mapping-pair-arrow" aria-hidden>→</span>
                <span className="df2-mapping-pair-target">{mapping.target}</span>
                <span className={`df2-mapping-pair-conf df2-mapping-pair-conf-${tier}`}>
                  {(mapping.confidence * 100).toFixed(0)}%
                </span>
                {mapping.isPii && (
                  <span className="df2-badge df2-badge-run df2-badge-xs">PII</span>
                )}
                {mapping.requiresReview && !mapping.approved && (
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

import { useMemo } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import type { IndexedMapping } from "../lib/columnWorkbench";
import { attentionMappings, mappingTier } from "../lib/columnWorkbench";
import type { EditableMapping } from "../lib/mapping";

interface MappingIntelligencePanelProps {
  allMappings: EditableMapping[];
  items: IndexedMapping[];
  sourceLabel: string;
  sourceSubtitle?: string;
  sourceType: string;
  destLabel: string;
  destSubtitle?: string;
  destType: string;
  confidenceThreshold?: number;
  totalCount?: number;
  mappedCount: number;
  llmUsed?: boolean;
  destSchemaLoading?: boolean;
  onSelectSource?: (source: string) => void;
  onFilterAttention?: (kind: "review" | "block" | "pii" | "warn") => void;
}

const ATTENTION_LABELS: Record<string, string> = {
  critical: "Critical",
  review: "Review",
  pii: "PII",
  warn: "Low confidence",
};

export function MappingIntelligencePanel({
  allMappings,
  items,
  sourceLabel,
  sourceSubtitle,
  sourceType,
  destLabel,
  destSubtitle,
  destType,
  confidenceThreshold = 0.85,
  totalCount,
  mappedCount,
  llmUsed,
  destSchemaLoading,
  onSelectSource,
  onFilterAttention,
}: MappingIntelligencePanelProps) {
  const showing = items.length;
  const total = totalCount ?? showing;

  const attention = useMemo(
    () => attentionMappings(allMappings, confidenceThreshold, 40),
    [allMappings, confidenceThreshold],
  );

  return (
    <aside className="df2-mapping-intelligence" aria-label="Mapping intelligence">
      <header className="df2-mapping-intelligence-head">
        <div className="df2-mapping-intelligence-title">
          <DtIcon name="sparkle" size={16} />
          <div>
            <strong>Mapping intelligence</strong>
            <span>Click any field to jump in the editor</span>
          </div>
        </div>
        {llmUsed && (
          <span className="df2-badge df2-badge-live df2-badge-xs">
            <DtIcon name="sparkle" size={10} /> LLM
          </span>
        )}
      </header>

      <div className="df2-mapping-intelligence-route df2-mapping-intelligence-route-compact">
        <div className="df2-mapping-intelligence-endpoint">
          <ConnectorIcon id={sourceType} size={18} />
          <div>
            <span>Source</span>
            <strong title={sourceLabel}>{sourceLabel}</strong>
          </div>
        </div>
        <span className="df2-mapping-route-arrow" aria-hidden>
          <DtIcon name="transfer" size={14} />
        </span>
        <div className="df2-mapping-intelligence-endpoint">
          <ConnectorIcon id={destType} size={18} />
          <div>
            <span>Destination</span>
            <strong title={destLabel}>{destLabel}</strong>
            {destSchemaLoading ? (
              <small>Loading destination schema…</small>
            ) : destSubtitle ? (
              <small>{destSubtitle}</small>
            ) : null}
          </div>
        </div>
      </div>

      <section className="df2-mapping-attention" aria-label="Fields needing attention">
        <div className="df2-mapping-attention-head">
          <DtIcon name="alert" size={14} />
          <strong>Needs attention</strong>
          <span className="df2-mapping-attention-count">{attention.length}</span>
        </div>
        {attention.length > 0 ? (
          <ul className="df2-mapping-attention-list">
            {attention.map(({ mapping, kind, tier }) => (
              <li key={`${kind}-${mapping.source}`}>
                <button
                  type="button"
                  className={`df2-mapping-attention-item tier-${tier} kind-${kind}`}
                  onClick={() => onSelectSource?.(mapping.source)}
                  title={`${mapping.source} → ${mapping.target}`}
                >
                  <span className={`df2-attention-kind df2-attention-kind-${kind}`}>
                    {ATTENTION_LABELS[kind]}
                  </span>
                  <span className="df2-attention-field">{mapping.source}</span>
                  <span className="df2-attention-conf">{(mapping.confidence * 100).toFixed(0)}%</span>
                </button>
              </li>
            ))}
          </ul>
        ) : (
          <p className="df2-mapping-attention-clear">
            <DtIcon name="check" size={14} /> All {mappedCount} fields are ready.
          </p>
        )}
        {attention.length > 0 && onFilterAttention && (
          <div className="df2-mapping-attention-filters">
            <button type="button" className="df2-btn df2-btn-sm" onClick={() => onFilterAttention("block")}>
              Critical
            </button>
            <button type="button" className="df2-btn df2-btn-sm" onClick={() => onFilterAttention("review")}>
              Review
            </button>
            <button type="button" className="df2-btn df2-btn-sm" onClick={() => onFilterAttention("pii")}>
              PII
            </button>
          </div>
        )}
      </section>

      <div className="df2-mapping-intelligence-pairs-head" aria-hidden>
        <span>Source</span>
        <span>→</span>
        <span>Destination</span>
        <span>%</span>
      </div>

      <div className="df2-mapping-intelligence-pairs-scroll">
        <ul className="df2-mapping-intelligence-pairs">
          {items.map(({ mapping, index }) => {
            const tier = mappingTier(mapping, confidenceThreshold);
            return (
              <li key={`${mapping.source}-${index}`}>
                <button
                  type="button"
                  className={`df2-mapping-intelligence-pair tier-${tier}`}
                  onClick={() => onSelectSource?.(mapping.source)}
                  title={`${mapping.reason || "Semantic match"} · ${(mapping.confidence * 100).toFixed(0)}%`}
                >
                  <span className="pair-source">{mapping.source}</span>
                  <span className="pair-arrow" aria-hidden>→</span>
                  <span className="pair-target">{mapping.target}</span>
                  <span className={`pair-conf conf-${tier}`}>{(mapping.confidence * 100).toFixed(0)}</span>
                  {mapping.isPii && <span className="df2-badge df2-badge-run df2-badge-xs">PII</span>}
                </button>
              </li>
            );
          })}
          {items.length === 0 && (
            <li className="df2-mapping-intelligence-empty">No pairs match the current filter.</li>
          )}
        </ul>
        {total > showing && (
          <p className="df2-mapping-intelligence-truncated">
            Showing {showing} of {total.toLocaleString()} pairs. Narrow filters in the editor.
          </p>
        )}
      </div>
    </aside>
  );
}

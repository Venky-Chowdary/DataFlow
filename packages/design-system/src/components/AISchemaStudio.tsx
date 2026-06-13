import { useState, useMemo } from "react";

interface ColumnSchema {
  name: string;
  type: string;
  nullable: boolean;
  samples: string[];
}

interface AIMapping {
  source: string;
  target: string;
  confidence: number;
  reasoning: string;
  transform?: string;
  status: "auto" | "confirmed" | "rejected" | "manual";
}

interface AISchemaStudioProps {
  sourceColumns: ColumnSchema[];
  targetColumns: ColumnSchema[];
  mappings: AIMapping[];
  onMappingChange?: (source: string, target: string) => void;
  onMappingConfirm?: (source: string) => void;
  onMappingReject?: (source: string) => void;
}

function ConfidenceRing({ value }: { value: number }) {
  const circumference = 2 * Math.PI * 18;
  const offset = circumference - (value * circumference);
  const color = value >= 0.95 ? "var(--dt-emerald)" : value >= 0.85 ? "var(--dt-electric)" : value >= 0.7 ? "var(--dt-amber)" : "var(--dt-coral)";

  return (
    <div className="dt-confidence-ring">
      <svg width="48" height="48" viewBox="0 0 48 48">
        <circle cx="24" cy="24" r="18" fill="none" stroke="rgba(255,255,255,0.1)" strokeWidth="3" />
        <circle
          cx="24"
          cy="24"
          r="18"
          fill="none"
          stroke={color}
          strokeWidth="3"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          transform="rotate(-90 24 24)"
          style={{ transition: "stroke-dashoffset 0.6s ease" }}
        />
      </svg>
      <span className="dt-confidence-ring-value" style={{ color }}>
        {Math.round(value * 100)}%
      </span>
    </div>
  );
}

function MappingLine({ mapping, sourceY, targetY }: { mapping: AIMapping; sourceY: number; targetY: number }) {
  const color = mapping.confidence >= 0.95 ? "#00FF9D" : mapping.confidence >= 0.85 ? "#00D4FF" : mapping.confidence >= 0.7 ? "#FFB800" : "#FF6B6B";
  const midX = 80;

  return (
    <g className="dt-mapping-line">
      <defs>
        <linearGradient id={`line-gradient-${mapping.source}`} x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor={color} stopOpacity="0.8" />
          <stop offset="50%" stopColor={color} stopOpacity="1" />
          <stop offset="100%" stopColor={color} stopOpacity="0.8" />
        </linearGradient>
        <filter id={`glow-${mapping.source}`}>
          <feGaussianBlur stdDeviation="3" result="coloredBlur" />
          <feMerge>
            <feMergeNode in="coloredBlur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>
      <path
        d={`M 0 ${sourceY} C ${midX} ${sourceY}, ${midX} ${targetY}, 160 ${targetY}`}
        fill="none"
        stroke={`url(#line-gradient-${mapping.source})`}
        strokeWidth="2"
        filter={`url(#glow-${mapping.source})`}
        className="dt-mapping-path"
      />
      <circle cx="80" cy={(sourceY + targetY) / 2} r="12" fill="rgba(7,11,20,0.9)" stroke={color} strokeWidth="1.5" />
      <text x="80" y={(sourceY + targetY) / 2 + 4} textAnchor="middle" fill={color} fontSize="8" fontWeight="600">AI</text>
    </g>
  );
}

export function AISchemaStudio({
  sourceColumns,
  targetColumns,
  mappings,
  onMappingChange,
  onMappingConfirm,
  onMappingReject,
}: AISchemaStudioProps) {
  const [selectedSource, setSelectedSource] = useState<string | null>(null);
  const [filter, setFilter] = useState<"all" | "review" | "confirmed">("all");

  const stats = useMemo(() => {
    const total = mappings.length;
    const autoMapped = mappings.filter((m) => m.status === "auto").length;
    const confirmed = mappings.filter((m) => m.status === "confirmed").length;
    const needsReview = mappings.filter((m) => m.confidence < 0.85 && m.status === "auto").length;
    const avgConfidence = mappings.reduce((sum, m) => sum + m.confidence, 0) / total;
    return { total, autoMapped, confirmed, needsReview, avgConfidence };
  }, [mappings]);

  const filteredMappings = useMemo(() => {
    if (filter === "review") return mappings.filter((m) => m.confidence < 0.85 && m.status === "auto");
    if (filter === "confirmed") return mappings.filter((m) => m.status === "confirmed" || m.status === "manual");
    return mappings;
  }, [mappings, filter]);

  return (
    <div className="dt-schema-studio">
      <div className="dt-schema-studio-header">
        <div className="dt-schema-studio-title-row">
          <div>
            <h2 className="dt-schema-studio-title">
              <span className="dt-schema-studio-title-icon">🧠</span>
              AI Schema Intelligence
            </h2>
            <p className="dt-schema-studio-subtitle">
              Intelligent semantic mapping with {Math.round(stats.avgConfidence * 100)}% average confidence
            </p>
          </div>
          <div className="dt-schema-studio-actions">
            <button className="dt-btn dt-btn-ghost">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <path d="M2 4H14M4 4V2H12V4M6 7V12M10 7V12M5 14H11L12 4H4L5 14Z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
              </svg>
              Clear All
            </button>
            <button className="dt-btn dt-btn-neon">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <path d="M8 2L9.5 5L13 5.5L10.5 8L11 11.5L8 10L5 11.5L5.5 8L3 5.5L6.5 5L8 2Z" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round"/>
              </svg>
              Re-analyze with AI
            </button>
          </div>
        </div>

        <div className="dt-schema-studio-stats">
          <div className="dt-schema-stat">
            <span className="dt-schema-stat-value">{stats.total}</span>
            <span className="dt-schema-stat-label">Total Columns</span>
          </div>
          <div className="dt-schema-stat dt-schema-stat--emerald">
            <span className="dt-schema-stat-value">{stats.autoMapped + stats.confirmed}</span>
            <span className="dt-schema-stat-label">Auto Mapped</span>
          </div>
          <div className="dt-schema-stat dt-schema-stat--amber">
            <span className="dt-schema-stat-value">{stats.needsReview}</span>
            <span className="dt-schema-stat-label">Needs Review</span>
          </div>
          <div className="dt-schema-stat dt-schema-stat--electric">
            <span className="dt-schema-stat-value">{Math.round(stats.avgConfidence * 100)}%</span>
            <span className="dt-schema-stat-label">Avg Confidence</span>
          </div>
        </div>

        <div className="dt-schema-studio-filters">
          {(["all", "review", "confirmed"] as const).map((f) => (
            <button
              key={f}
              type="button"
              className={`dt-schema-filter ${filter === f ? "dt-schema-filter--active" : ""}`}
              onClick={() => setFilter(f)}
            >
              {f === "all" ? "All Mappings" : f === "review" ? `Needs Review (${stats.needsReview})` : "Confirmed"}
            </button>
          ))}
        </div>
      </div>

      <div className="dt-schema-studio-canvas">
        <div className="dt-schema-panel dt-schema-panel--source">
          <div className="dt-schema-panel-header">
            <span className="dt-schema-panel-badge dt-schema-panel-badge--source">Source</span>
            <span className="dt-schema-panel-count">{sourceColumns.length} columns</span>
          </div>
          <div className="dt-schema-columns">
            {filteredMappings.map((mapping) => {
              const col = sourceColumns.find((c) => c.name === mapping.source);
              if (!col) return null;
              return (
                <div
                  key={mapping.source}
                  className={`dt-schema-column ${selectedSource === mapping.source ? "dt-schema-column--selected" : ""} ${mapping.confidence < 0.85 ? "dt-schema-column--review" : ""}`}
                  onClick={() => setSelectedSource(mapping.source)}
                >
                  <div className="dt-schema-column-name">
                    <span className="dt-schema-column-name-text">{col.name}</span>
                    <span className="dt-schema-column-type">{col.type}</span>
                  </div>
                  {col.samples.length > 0 && (
                    <div className="dt-schema-column-samples">
                      {col.samples.slice(0, 2).map((s, i) => (
                        <span key={i} className="dt-schema-column-sample">{s}</span>
                      ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>

        <div className="dt-schema-mapping-canvas">
          <div className="dt-schema-mapping-header">
            <span className="dt-schema-mapping-badge">🤖 AI Engine</span>
          </div>
          <svg className="dt-schema-lines" viewBox="0 0 160 600" preserveAspectRatio="none">
            {filteredMappings.map((mapping, i) => (
              <MappingLine
                key={mapping.source}
                mapping={mapping}
                sourceY={40 + i * 70}
                targetY={40 + i * 70}
              />
            ))}
          </svg>
        </div>

        <div className="dt-schema-panel dt-schema-panel--target">
          <div className="dt-schema-panel-header">
            <span className="dt-schema-panel-badge dt-schema-panel-badge--target">Destination</span>
            <span className="dt-schema-panel-count">{targetColumns.length} columns</span>
          </div>
          <div className="dt-schema-columns">
            {filteredMappings.map((mapping) => {
              const col = targetColumns.find((c) => c.name === mapping.target);
              const targetName = col?.name || mapping.target;
              const targetType = col?.type || "VARCHAR";
              return (
                <div
                  key={mapping.source}
                  className={`dt-schema-column ${mapping.confidence < 0.85 ? "dt-schema-column--review" : ""}`}
                >
                  <div className="dt-schema-column-name">
                    <span className="dt-schema-column-name-text">{targetName}</span>
                    <span className="dt-schema-column-type">{targetType}</span>
                  </div>
                  <ConfidenceRing value={mapping.confidence} />
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {selectedSource && (
        <div className="dt-mapping-detail">
          {(() => {
            const mapping = mappings.find((m) => m.source === selectedSource);
            if (!mapping) return null;
            return (
              <>
                <div className="dt-mapping-detail-header">
                  <h3 className="dt-mapping-detail-title">Mapping Details</h3>
                  <button className="dt-btn dt-btn-ghost dt-btn-icon" onClick={() => setSelectedSource(null)}>
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                      <path d="M4 4L12 12M12 4L4 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
                    </svg>
                  </button>
                </div>
                <div className="dt-mapping-detail-flow">
                  <div className="dt-mapping-detail-node">
                    <span className="dt-mapping-detail-label">Source</span>
                    <span className="dt-mapping-detail-value">{mapping.source}</span>
                  </div>
                  <div className="dt-mapping-detail-arrow">→</div>
                  <div className="dt-mapping-detail-node dt-mapping-detail-node--target">
                    <span className="dt-mapping-detail-label">Target</span>
                    <span className="dt-mapping-detail-value">{mapping.target}</span>
                  </div>
                </div>
                <div className="dt-mapping-detail-confidence">
                  <ConfidenceRing value={mapping.confidence} />
                  <div>
                    <span className="dt-mapping-detail-confidence-label">AI Confidence</span>
                    <p className="dt-mapping-detail-reasoning">{mapping.reasoning}</p>
                  </div>
                </div>
                {mapping.transform && (
                  <div className="dt-mapping-detail-transform">
                    <span className="dt-mapping-detail-label">Transform</span>
                    <code className="dt-mapping-detail-code">{mapping.transform}</code>
                  </div>
                )}
                <div className="dt-mapping-detail-actions">
                  <button
                    className="dt-btn dt-btn-secondary"
                    onClick={() => onMappingReject?.(mapping.source)}
                  >
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                      <path d="M4 4L12 12M12 4L4 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
                    </svg>
                    Reject
                  </button>
                  <button
                    className="dt-btn dt-btn-primary"
                    onClick={() => onMappingConfirm?.(mapping.source)}
                  >
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                      <path d="M3 8L6.5 11.5L13 5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                    </svg>
                    Confirm Mapping
                  </button>
                </div>
              </>
            );
          })()}
        </div>
      )}
    </div>
  );
}

export function AISchemaStudioStyles() {
  return (
    <style>{`
      .dt-schema-studio {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-6);
      }

      .dt-schema-studio-header {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-5);
      }

      .dt-schema-studio-title-row {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: var(--dt-space-4);
      }

      .dt-schema-studio-title {
        display: flex;
        align-items: center;
        gap: var(--dt-space-3);
        font-size: var(--dt-text-2xl);
        font-weight: 700;
        color: var(--dt-text);
      }

      .dt-schema-studio-title-icon {
        font-size: 28px;
      }

      .dt-schema-studio-subtitle {
        font-size: var(--dt-text-sm);
        color: var(--dt-text-tertiary);
        margin-top: var(--dt-space-1);
      }

      .dt-schema-studio-actions {
        display: flex;
        gap: var(--dt-space-3);
      }

      .dt-schema-studio-stats {
        display: flex;
        gap: var(--dt-space-6);
        padding: var(--dt-space-5);
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-schema-stat {
        display: flex;
        flex-direction: column;
        gap: 4px;
      }

      .dt-schema-stat-value {
        font-size: var(--dt-text-2xl);
        font-weight: 700;
        color: var(--dt-text);
        font-variant-numeric: tabular-nums;
      }

      .dt-schema-stat-label {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-tertiary);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      .dt-schema-stat--emerald .dt-schema-stat-value { color: var(--dt-emerald); }
      .dt-schema-stat--amber .dt-schema-stat-value { color: var(--dt-amber); }
      .dt-schema-stat--electric .dt-schema-stat-value { color: var(--dt-electric); }

      .dt-schema-studio-filters {
        display: flex;
        gap: var(--dt-space-2);
      }

      .dt-schema-filter {
        padding: var(--dt-space-2) var(--dt-space-4);
        font-family: inherit;
        font-size: var(--dt-text-sm);
        font-weight: 500;
        color: var(--dt-text-secondary);
        background: transparent;
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-md);
        cursor: pointer;
        transition: all var(--dt-duration-fast) var(--dt-ease);
      }

      .dt-schema-filter:hover {
        border-color: var(--dt-border-strong);
        color: var(--dt-text);
      }

      .dt-schema-filter--active {
        background: var(--dt-electric-dim);
        border-color: var(--dt-electric);
        color: var(--dt-electric);
      }

      .dt-schema-studio-canvas {
        display: grid;
        grid-template-columns: 1fr 160px 1fr;
        gap: 0;
        min-height: 500px;
        background: var(--dt-surface);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-2xl);
        overflow: hidden;
      }

      .dt-schema-panel {
        display: flex;
        flex-direction: column;
      }

      .dt-schema-panel-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: var(--dt-space-4) var(--dt-space-5);
        border-bottom: 1px solid var(--dt-border);
        background: rgba(0, 0, 0, 0.2);
      }

      .dt-schema-panel-badge {
        padding: 4px 12px;
        font-size: var(--dt-text-xs);
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        border-radius: var(--dt-radius-full);
      }

      .dt-schema-panel-badge--source {
        background: rgba(255, 184, 0, 0.15);
        color: var(--dt-amber);
      }

      .dt-schema-panel-badge--target {
        background: var(--dt-emerald-dim);
        color: var(--dt-emerald);
      }

      .dt-schema-panel-count {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-muted);
      }

      .dt-schema-columns {
        flex: 1;
        overflow-y: auto;
        padding: var(--dt-space-3);
      }

      .dt-schema-column {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: var(--dt-space-3);
        padding: var(--dt-space-3) var(--dt-space-4);
        margin-bottom: var(--dt-space-2);
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid transparent;
        border-radius: var(--dt-radius-lg);
        cursor: pointer;
        transition: all var(--dt-duration-fast) var(--dt-ease);
      }

      .dt-schema-column:hover {
        background: rgba(255, 255, 255, 0.05);
        border-color: var(--dt-border);
      }

      .dt-schema-column--selected {
        background: var(--dt-electric-dim);
        border-color: var(--dt-electric);
      }

      .dt-schema-column--review {
        border-color: rgba(255, 184, 0, 0.3);
      }

      .dt-schema-column-name {
        display: flex;
        flex-direction: column;
        gap: 2px;
        min-width: 0;
      }

      .dt-schema-column-name-text {
        font-family: var(--dt-font-mono);
        font-size: var(--dt-text-sm);
        font-weight: 500;
        color: var(--dt-text);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .dt-schema-column-type {
        font-size: var(--dt-text-xs);
        color: var(--dt-text-muted);
        text-transform: uppercase;
      }

      .dt-schema-column-samples {
        display: flex;
        gap: var(--dt-space-1);
      }

      .dt-schema-column-sample {
        padding: 2px 6px;
        font-family: var(--dt-font-mono);
        font-size: 10px;
        color: var(--dt-text-tertiary);
        background: rgba(255, 255, 255, 0.05);
        border-radius: var(--dt-radius-sm);
      }

      .dt-schema-mapping-canvas {
        display: flex;
        flex-direction: column;
        background: linear-gradient(180deg, rgba(0, 212, 255, 0.03), rgba(123, 97, 255, 0.03));
        border-left: 1px solid var(--dt-border);
        border-right: 1px solid var(--dt-border);
      }

      .dt-schema-mapping-header {
        display: flex;
        justify-content: center;
        padding: var(--dt-space-4);
        border-bottom: 1px solid var(--dt-border);
        background: rgba(0, 0, 0, 0.2);
      }

      .dt-schema-mapping-badge {
        padding: 4px 12px;
        font-size: var(--dt-text-xs);
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        background: var(--dt-purple-dim);
        color: var(--dt-purple);
        border-radius: var(--dt-radius-full);
      }

      .dt-schema-lines {
        flex: 1;
        width: 100%;
      }

      .dt-mapping-path {
        stroke-dasharray: 8 4;
        animation: dt-flow-dash 1s linear infinite;
      }

      @keyframes dt-flow-dash {
        to { stroke-dashoffset: -24; }
      }

      .dt-confidence-ring {
        position: relative;
        width: 48px;
        height: 48px;
        flex-shrink: 0;
      }

      .dt-confidence-ring-value {
        position: absolute;
        inset: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 11px;
        font-weight: 700;
      }

      .dt-mapping-detail {
        padding: var(--dt-space-6);
        background: var(--dt-surface);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-mapping-detail-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: var(--dt-space-5);
      }

      .dt-mapping-detail-title {
        font-size: var(--dt-text-md);
        font-weight: 600;
        color: var(--dt-text);
      }

      .dt-mapping-detail-flow {
        display: flex;
        align-items: center;
        gap: var(--dt-space-4);
        padding: var(--dt-space-4);
        background: rgba(255, 255, 255, 0.02);
        border-radius: var(--dt-radius-lg);
        margin-bottom: var(--dt-space-5);
      }

      .dt-mapping-detail-node {
        flex: 1;
        padding: var(--dt-space-4);
        background: rgba(255, 184, 0, 0.1);
        border: 1px solid rgba(255, 184, 0, 0.3);
        border-radius: var(--dt-radius-md);
      }

      .dt-mapping-detail-node--target {
        background: var(--dt-emerald-dim);
        border-color: rgba(0, 255, 157, 0.3);
      }

      .dt-mapping-detail-label {
        display: block;
        font-size: var(--dt-text-xs);
        color: var(--dt-text-muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 4px;
      }

      .dt-mapping-detail-value {
        font-family: var(--dt-font-mono);
        font-size: var(--dt-text-md);
        font-weight: 600;
        color: var(--dt-text);
      }

      .dt-mapping-detail-arrow {
        font-size: 24px;
        color: var(--dt-text-muted);
      }

      .dt-mapping-detail-confidence {
        display: flex;
        align-items: flex-start;
        gap: var(--dt-space-4);
        margin-bottom: var(--dt-space-5);
      }

      .dt-mapping-detail-confidence-label {
        display: block;
        font-size: var(--dt-text-sm);
        font-weight: 600;
        color: var(--dt-text);
        margin-bottom: 4px;
      }

      .dt-mapping-detail-reasoning {
        font-size: var(--dt-text-sm);
        color: var(--dt-text-secondary);
        line-height: 1.5;
      }

      .dt-mapping-detail-transform {
        padding: var(--dt-space-4);
        background: rgba(0, 0, 0, 0.3);
        border-radius: var(--dt-radius-md);
        margin-bottom: var(--dt-space-5);
      }

      .dt-mapping-detail-code {
        display: block;
        font-family: var(--dt-font-mono);
        font-size: var(--dt-text-sm);
        color: var(--dt-electric);
        margin-top: var(--dt-space-2);
      }

      .dt-mapping-detail-actions {
        display: flex;
        justify-content: flex-end;
        gap: var(--dt-space-3);
      }
    `}</style>
  );
}

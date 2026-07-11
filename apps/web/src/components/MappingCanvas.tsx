import { useEffect, useRef, useState } from "react";
import { DtIcon } from "./DtIcon";
import { normalizeMappingTarget } from "../lib/mapping";
import { ColumnAnalysis } from "../lib/types";

export interface MappingLink {
  source: string;
  target: string;
  confidence: number;
  semanticType?: string;
  isPii?: boolean;
  compliance?: string[];
}

const DENSE_COLUMN_THRESHOLD = 6;

interface MappingCanvasProps {
  columns: ColumnAnalysis[];
  links?: MappingLink[];
  sourceLabel?: string;
  sourceSubtitle?: string;
  destinationLabel?: string;
  destinationSubtitle?: string;
  targetTable?: string;
  onFixLowConfidence?: () => void;
  /** Hide chrome — used when embedded in transfer map step */
  minimal?: boolean;
}

function confidenceTier(c: number): "high" | "medium" | "low" {
  if (c >= 0.9) return "high";
  if (c >= 0.7) return "medium";
  return "low";
}

export function MappingCanvas({
  columns,
  links: linksProp,
  sourceLabel = "Source",
  sourceSubtitle,
  destinationLabel = "Destination",
  destinationSubtitle,
  targetTable,
  onFixLowConfidence,
  minimal = false,
}: MappingCanvasProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const sourceColsRef = useRef<HTMLDivElement>(null);
  const targetColsRef = useRef<HTMLDivElement>(null);
  const sourceRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  const targetRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  const [lines, setLines] = useState<{ x1: number; y1: number; x2: number; y2: number; confidence: number; source: string }[]>([]);

  const links: MappingLink[] = linksProp ?? columns.map((col) => ({
    source: col.column_name,
    target: normalizeMappingTarget(col.column_name, col),
    confidence: col.confidence,
    semanticType: col.semantic_type ?? col.inferred_type,
    isPii: col.is_pii,
    compliance: col.compliance,
  }));

  const columnBySource = new Map(columns.map((col) => [col.column_name, col]));
  const lowCount = links.filter((l) => l.confidence < 0.7).length;
  const reviewCount = links.filter((l) => l.confidence < 0.85).length;
  const piiCount = links.filter((l) => l.isPii).length;
  const avgConfidence = links.length
    ? links.reduce((s, l) => s + l.confidence, 0) / links.length
    : 0;
  const ragValidated = columns.filter((col) => (col.rag_confidence ?? 0) >= 0.8).length;
  const deterministicCount = columns.filter((col) => (col.method ?? "").includes("pattern") || col.confidence >= 0.9).length;

  const isDense = links.length > DENSE_COLUMN_THRESHOLD;
  const showConnectorLines = !isDense && !minimal;

  useEffect(() => {
    if (!showConnectorLines) {
      setLines([]);
      return;
    }

    const measure = () => {
      const container = containerRef.current;
      if (!container) return;
      const rect = container.getBoundingClientRect();
      const next: typeof lines = [];

      for (const link of links) {
        const src = sourceRefs.current.get(link.source);
        const tgt = targetRefs.current.get(link.target);
        if (!src || !tgt) continue;
        const sr = src.getBoundingClientRect();
        const tr = tgt.getBoundingClientRect();
        if (sr.bottom < rect.top || sr.top > rect.bottom) continue;
        if (tr.bottom < rect.top || tr.top > rect.bottom) continue;
        next.push({
          x1: sr.right - rect.left,
          y1: sr.top + sr.height / 2 - rect.top,
          x2: tr.left - rect.left,
          y2: tr.top + tr.height / 2 - rect.top,
          confidence: link.confidence,
          source: link.source,
        });
      }
      setLines(next);
    };

    const t = window.setTimeout(measure, 50);
    const ro = new ResizeObserver(measure);
    const container = containerRef.current;
    const sourceEl = sourceColsRef.current;
    const targetEl = targetColsRef.current;
    if (container) ro.observe(container);
    window.addEventListener("resize", measure);
    sourceEl?.addEventListener("scroll", measure, { passive: true });
    targetEl?.addEventListener("scroll", measure, { passive: true });
    return () => {
      window.clearTimeout(t);
      ro.disconnect();
      window.removeEventListener("resize", measure);
      sourceEl?.removeEventListener("scroll", measure);
      targetEl?.removeEventListener("scroll", measure);
    };
  }, [links, showConnectorLines]);

  const uniqueTargets = [...new Set(links.map((l) => l.target))];

  return (
    <div className={`df2-mapping ${isDense ? "is-dense" : ""} ${minimal ? "is-minimal" : ""}`}>
      {!minimal && (
      <div className="df2-mapping-head">
        <div>
          <h3 className="df2-mapping-title">Schema Mapping Workbench</h3>
          <p className="df2-mapping-sub">
            {links.length} columns mapped · {(avgConfidence * 100).toFixed(0)}% avg confidence
            {piiCount > 0 && ` · ${piiCount} PII detected`}
            {isDense && " · use the table below to review wide schemas"}
          </p>
        </div>
        <div className="df2-segment">
          {piiCount > 0 && (
            <span className="df2-badge df2-badge-run">
              <DtIcon name="shield" size={12} /> {piiCount} PII
            </span>
          )}
          {lowCount > 0 && onFixLowConfidence && (
            <button type="button" className="df2-btn df2-btn-sm" onClick={onFixLowConfidence}>
              <DtIcon name="sparkle" size={14} /> Fix {lowCount} low-confidence
            </button>
          )}
        </div>
      </div>
      )}

      {!minimal && (
      <div className="df2-assurance-strip" aria-label="Mapping assurance">
        <div className="df2-assurance-chip">
          <span>Assignment</span>
          <strong>{links.length} mapped</strong>
        </div>
        <div className="df2-assurance-chip">
          <span>Evidence</span>
          <strong>{ragValidated}/{links.length} RAG checked</strong>
        </div>
        <div className="df2-assurance-chip">
          <span>Deterministic</span>
          <strong>{deterministicCount}/{links.length} rules</strong>
        </div>
        <div className={`df2-assurance-chip ${reviewCount ? "warn" : "ok"}`}>
          <span>Review</span>
          <strong>{reviewCount ? `${reviewCount} columns` : "Clear"}</strong>
        </div>
      </div>
      )}

      <div className="df2-mapping-body" ref={containerRef}>
        {showConnectorLines && (
        <svg className="df2-mapping-svg" aria-hidden>
          {lines.map((line) => {
            const tier = confidenceTier(line.confidence);
            const midX = (line.x1 + line.x2) / 2;
            const path = `M ${line.x1} ${line.y1} C ${midX} ${line.y1}, ${midX} ${line.y2}, ${line.x2} ${line.y2}`;
            return (
              <g key={line.source} className={`df2-map-line-${tier}`}>
                <path
                  d={path}
                  fill="none"
                  strokeWidth={tier === "high" ? 2.5 : tier === "medium" ? 2 : 1.5}
                  strokeDasharray={tier === "low" ? "6 4" : undefined}
                  className="df2-map-line-path"
                />
              </g>
            );
          })}
        </svg>
        )}

        <div className="df2-mapping-pane df2-mapping-pane-source">
          <div className="df2-mapping-pane-head">
            <div className="df2-mapping-pane-label">
              <DtIcon name="upload" size={14} /> {sourceLabel}
            </div>
            {sourceSubtitle && <p className="df2-mapping-pane-sub">{sourceSubtitle}</p>}
          </div>
          <div className="df2-mapping-cols" ref={sourceColsRef}>
            {links.map((link) => (
              <div
                key={link.source}
                ref={(el) => { if (el) sourceRefs.current.set(link.source, el); }}
                className={`df2-mapping-col ${link.isPii ? "pii" : ""} ${confidenceTier(link.confidence)}`}
              >
                <span className="df2-mapping-col-name">{link.source}</span>
                <span className="df2-mapping-col-type">{link.semanticType ?? "—"}</span>
                {!isDense && (columnBySource.get(link.source)?.method || columnBySource.get(link.source)?.rag_confidence) && (
                  <span className="df2-mapping-evidence">
                    {columnBySource.get(link.source)?.method ?? "semantic"}
                    {columnBySource.get(link.source)?.rag_confidence
                      ? ` · ${Math.round((columnBySource.get(link.source)?.rag_confidence ?? 0) * 100)}% evidence`
                      : ""}
                  </span>
                )}
                {link.isPii && (
                  <span className="df2-mapping-pii" title={link.compliance?.join(", ")}>PII</span>
                )}
              </div>
            ))}
          </div>
        </div>

        {!isDense && (
          <div className="df2-mapping-engine">
            <div className="df2-mapping-engine-ring">
              <DtIcon name="sparkle" size={20} />
            </div>
            <span>AI Engine</span>
          </div>
        )}

        <div className={`df2-mapping-pane df2-mapping-pane-dest ${isDense ? "is-wide" : ""}`}>
          <div className="df2-mapping-pane-head">
            <div className="df2-mapping-pane-label">
              <DtIcon name="connectors" size={14} /> {destinationLabel}
              {targetTable && <span className="df2-mapping-table-chip">{targetTable}</span>}
            </div>
            {destinationSubtitle && <p className="df2-mapping-pane-sub">{destinationSubtitle}</p>}
          </div>
          <div className="df2-mapping-cols" ref={targetColsRef}>
            {uniqueTargets.map((target) => {
              const link = links.find((l) => l.target === target)!;
              return (
              <div
                key={target}
                ref={(el) => { if (el) targetRefs.current.set(target, el); }}
                className={`df2-mapping-col ${confidenceTier(link.confidence)}`}
              >
                <span className="df2-mapping-col-name">{target}</span>
                {!isDense && (
                  <>
                    <div className="df2-conf-bar">
                      <div className="df2-conf-track">
                        <div
                          className={`df2-conf-fill ${confidenceTier(link.confidence)}`}
                          style={{ width: `${Math.min(link.confidence * 100, 100)}%` }}
                        />
                      </div>
                      <span className="df2-conf-val">{(link.confidence * 100).toFixed(0)}%</span>
                    </div>
                    <span className="df2-mapping-evidence">
                      {confidenceTier(link.confidence) === "high" ? "auto-approved" : "review"}
                    </span>
                  </>
                )}
                {isDense && (
                  <span className="df2-conf-val">{(link.confidence * 100).toFixed(0)}%</span>
                )}
              </div>
              );
            })}
          </div>
        </div>
      </div>

      <div className="df2-mapping-mobile" aria-label="Column mappings">
        {links.map((link) => (
          <div key={link.source} className="df2-mapping-mobile-row">
            <span>{link.source}</span>
            <span>→</span>
            <span>{link.target}</span>
            <span className="df2-mapping-mobile-meta">
              {(link.confidence * 100).toFixed(0)}% · {link.semanticType ?? "mapped"}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

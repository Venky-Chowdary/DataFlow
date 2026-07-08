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

interface MappingCanvasProps {
  columns: ColumnAnalysis[];
  destinationLabel?: string;
  targetTable?: string;
  onFixLowConfidence?: () => void;
}

function confidenceTier(c: number): "high" | "medium" | "low" {
  if (c >= 0.9) return "high";
  if (c >= 0.7) return "medium";
  return "low";
}

export function MappingCanvas({
  columns,
  destinationLabel = "Destination",
  targetTable,
  onFixLowConfidence,
}: MappingCanvasProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const sourceRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  const targetRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  const [lines, setLines] = useState<{ x1: number; y1: number; x2: number; y2: number; confidence: number; source: string }[]>([]);

  const links: MappingLink[] = columns.map((col) => ({
    source: col.column_name,
    target: normalizeMappingTarget(col.column_name, col),
    confidence: col.confidence,
    semanticType: col.semantic_type ?? col.inferred_type,
    isPii: col.is_pii,
    compliance: col.compliance,
  }));

  const lowCount = links.filter((l) => l.confidence < 0.7).length;
  const reviewCount = columns.filter((col) => col.confidence < 0.85 || (col.rag_confidence ?? col.confidence) < 0.7).length;
  const piiCount = links.filter((l) => l.isPii).length;
  const avgConfidence = links.length
    ? links.reduce((s, l) => s + l.confidence, 0) / links.length
    : 0;
  const ragValidated = columns.filter((col) => (col.rag_confidence ?? 0) >= 0.8).length;
  const deterministicCount = columns.filter((col) => (col.method ?? "").includes("pattern") || col.confidence >= 0.9).length;

  useEffect(() => {
    const measure = () => {
      const container = containerRef.current;
      if (!container) return;
      const rect = container.getBoundingClientRect();
      const next: typeof lines = [];

      for (const col of columns) {
        const target = normalizeMappingTarget(col.column_name, col);
        const src = sourceRefs.current.get(col.column_name);
        const tgt = targetRefs.current.get(target);
        if (!src || !tgt) continue;
        const sr = src.getBoundingClientRect();
        const tr = tgt.getBoundingClientRect();
        next.push({
          x1: sr.right - rect.left,
          y1: sr.top + sr.height / 2 - rect.top,
          x2: tr.left - rect.left,
          y2: tr.top + tr.height / 2 - rect.top,
          confidence: col.confidence,
          source: col.column_name,
        });
      }
      setLines(next);
    };

    const t = window.setTimeout(measure, 50);
    const ro = new ResizeObserver(measure);
    if (containerRef.current) ro.observe(containerRef.current);
    window.addEventListener("resize", measure);
    return () => {
      window.clearTimeout(t);
      ro.disconnect();
      window.removeEventListener("resize", measure);
    };
  }, [columns]);

  return (
    <div className="df2-mapping">
      <div className="df2-mapping-head">
        <div>
          <h3 className="df2-mapping-title">Schema Mapping Workbench</h3>
          <p className="df2-mapping-sub">
            {links.length} columns mapped · {(avgConfidence * 100).toFixed(0)}% avg confidence
            {piiCount > 0 && ` · ${piiCount} PII detected`}
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

      <div className="df2-assurance-strip" aria-label="Mapping assurance">
        <div className="df2-assurance-chip">
          <span>Assignment</span>
          <strong>Optimal one-to-one</strong>
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

      <div className="df2-mapping-body" ref={containerRef}>
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
                />
                <circle r="3" className="df2-map-particle">
                  <animateMotion dur={`${2 + line.confidence * 2}s`} repeatCount="indefinite" path={path} />
                </circle>
              </g>
            );
          })}
        </svg>

        <div className="df2-mapping-pane">
          <div className="df2-mapping-pane-label">
            <DtIcon name="upload" size={14} /> Source
          </div>
          <div className="df2-mapping-cols">
            {links.map((link) => (
              <div
                key={link.source}
                ref={(el) => { if (el) sourceRefs.current.set(link.source, el); }}
                className={`df2-mapping-col ${link.isPii ? "pii" : ""} ${confidenceTier(link.confidence)}`}
              >
                <span className="df2-mapping-col-name">{link.source}</span>
                <span className="df2-mapping-col-type">{link.semanticType ?? "—"}</span>
                {(columns.find((col) => col.column_name === link.source)?.method || columns.find((col) => col.column_name === link.source)?.rag_confidence) && (
                  <span className="df2-mapping-evidence">
                    {columns.find((col) => col.column_name === link.source)?.method ?? "semantic"}
                    {columns.find((col) => col.column_name === link.source)?.rag_confidence
                      ? ` · ${Math.round((columns.find((col) => col.column_name === link.source)?.rag_confidence ?? 0) * 100)}% evidence`
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

        <div className="df2-mapping-engine">
          <div className="df2-mapping-engine-ring">
            <DtIcon name="sparkle" size={20} />
          </div>
          <span>AI Engine</span>
        </div>

        <div className="df2-mapping-pane">
          <div className="df2-mapping-pane-label">
            <DtIcon name="connectors" size={14} /> {destinationLabel}
            {targetTable && <span className="df2-mapping-table-chip">{targetTable}</span>}
          </div>
          <div className="df2-mapping-cols">
            {links.map((link) => (
              <div
                key={link.target}
                ref={(el) => { if (el) targetRefs.current.set(link.target, el); }}
                className={`df2-mapping-col ${confidenceTier(link.confidence)}`}
              >
                <span className="df2-mapping-col-name">{link.target}</span>
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
              </div>
            ))}
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

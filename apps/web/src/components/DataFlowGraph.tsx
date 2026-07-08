import { useEffect, useRef, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtLogo } from "./DtLogo";

export interface FlowNode {
  id: string;
  label: string;
  type: string;
  active?: boolean;
}

interface DataFlowGraphProps {
  nodes: FlowNode[];
  centerLabel?: string;
  emptyHint?: string;
}

const VB = { w: 400, h: 280, cx: 200, cy: 140, rx: 120, ry: 90 };

function layoutNode(i: number, count: number) {
  const angle = (i / count) * Math.PI * 2 - Math.PI / 2;
  const cx = VB.cx + Math.cos(angle) * VB.rx;
  const cy = VB.cy + Math.sin(angle) * VB.ry;
  return { cx, cy, angle, pathId: `flow-path-${i}` };
}

function curvePath(x1: number, y1: number, x2: number, y2: number) {
  const mx = (x1 + x2) / 2;
  const my = (y1 + y2) / 2;
  const dx = x2 - x1;
  const dy = y2 - y1;
  const cx = mx - dy * 0.15;
  const cy = my + dx * 0.15;
  return `M ${x1} ${y1} Q ${cx} ${cy} ${x2} ${y2}`;
}

/** Map viewBox coords → pixel coords inside the measured canvas */
function toPixel(cx: number, cy: number, width: number, height: number) {
  return { x: (cx / VB.w) * width, y: (cy / VB.h) * height };
}

export function DataFlowGraph({ nodes, centerLabel = "DataFlow", emptyHint }: DataFlowGraphProps) {
  const canvasRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 400, h: 280 });
  const activeNodes = nodes.filter((n) => n.active !== false);
  const count = Math.max(activeNodes.length, 1);

  useEffect(() => {
    const el = canvasRef.current;
    if (!el) return;
    const measure = () => {
      const rect = el.getBoundingClientRect();
      if (rect.width > 0 && rect.height > 0) {
        setSize({ w: rect.width, h: rect.height });
      }
    };
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const hub = toPixel(VB.cx, VB.cy, size.w, size.h);

  return (
    <div className="dt-flow-graph" aria-label="Connected data flow topology">
      <div className="dt-flow-graph-canvas" ref={canvasRef}>
        <svg
          className="dt-flow-graph-svg"
          width={size.w}
          height={size.h}
          viewBox={`0 0 ${size.w} ${size.h}`}
          preserveAspectRatio="none"
        >
          <defs>
            <linearGradient id="dt-flow-line" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor="var(--dt-flow-start, #3b82f6)" />
              <stop offset="100%" stopColor="var(--dt-flow-end, #0891b2)" />
            </linearGradient>
            <filter id="dt-flow-glow">
              <feGaussianBlur stdDeviation="2.5" result="blur" />
              <feMerge>
                <feMergeNode in="blur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
            <radialGradient id="dt-flow-hub-grad" cx="50%" cy="50%" r="50%">
              <stop offset="0%" stopColor="var(--df-brand-muted)" />
              <stop offset="100%" stopColor="var(--df-surface)" />
            </radialGradient>
          </defs>

          {activeNodes.length === 0 && (
            <text x={hub.x} y={hub.y} textAnchor="middle" dominantBaseline="middle" className="dt-flow-empty-text">
              {emptyHint || "Connect sources & destinations to see live data flow"}
            </text>
          )}

          {activeNodes.map((node, i) => {
            const { cx, cy, pathId } = layoutNode(i, count);
            const start = toPixel(cx, cy, size.w, size.h);
            const d = curvePath(start.x, start.y, hub.x, hub.y);
            return (
              <g key={node.id}>
                <path
                  id={pathId}
                  d={d}
                  fill="none"
                  stroke="url(#dt-flow-line)"
                  strokeWidth="2.5"
                  strokeDasharray="8 6"
                  className="dt-flow-path"
                  opacity={0.7}
                />
                <circle r="4" fill="var(--dt-flow-particle, #2563eb)" filter="url(#dt-flow-glow)">
                  <animateMotion dur={`${2.4 + (i % 3) * 0.35}s`} repeatCount="indefinite" rotate="auto">
                    <mpath href={`#${pathId}`} />
                  </animateMotion>
                </circle>
              </g>
            );
          })}

          <circle cx={hub.x} cy={hub.y} r={Math.min(size.w, size.h) * 0.11} className="dt-flow-hub-ring" fill="none" />
          <circle cx={hub.x} cy={hub.y} r={Math.min(size.w, size.h) * 0.085} fill="url(#dt-flow-hub-grad)" className="dt-flow-hub-core" />
        </svg>

        <div className="dt-flow-hub-center" style={{ left: hub.x, top: hub.y }}>
          <DtLogo size={Math.round(Math.min(size.w, size.h) * 0.14)} />
          <span>{centerLabel}</span>
        </div>

        {activeNodes.map((node, i) => {
          const { cx, cy } = layoutNode(i, count);
          const pos = toPixel(cx, cy, size.w, size.h);
          return (
            <div
              key={node.id}
              className="dt-flow-node"
              style={{ left: pos.x, top: pos.y }}
              title={node.label}
            >
              <div className="dt-flow-node-icon">
                <ConnectorIcon id={node.type} size={Math.round(Math.min(size.w, size.h) * 0.08)} />
              </div>
              <span className="dt-flow-node-label">{node.label}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

import { useEffect, useRef, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtLogo } from "./DtLogo";

export interface HubNode {
  id: string;
  label: string;
  type: string;
  active?: boolean;
}

interface ConnectionHubProps {
  nodes: HubNode[];
  centerLabel?: string;
  emptyHint?: string;
  variant?: "default" | "hero";
}

const VB = { w: 520, h: 360, cx: 260, cy: 180, rx: 168, ry: 118 };

function layoutNode(i: number, count: number) {
  const angle = (i / count) * Math.PI * 2 - Math.PI / 2;
  return {
    cx: VB.cx + Math.cos(angle) * VB.rx,
    cy: VB.cy + Math.sin(angle) * VB.ry,
    angle,
    pathId: `hub-path-${i}`,
  };
}

function toPixel(cx: number, cy: number, w: number, h: number) {
  return { x: (cx / VB.w) * w, y: (cy / VB.h) * h };
}

function curvePath(x1: number, y1: number, x2: number, y2: number) {
  const mx = (x1 + x2) / 2;
  const my = (y1 + y2) / 2;
  const dx = x2 - x1;
  const dy = y2 - y1;
  const cx = mx - dy * 0.22;
  const cy = my + dx * 0.22;
  return `M ${x1} ${y1} Q ${cx} ${cy} ${x2} ${y2}`;
}

export function ConnectionHub({
  nodes,
  centerLabel = "DataFlow",
  emptyHint,
  variant = "default",
}: ConnectionHubProps) {
  const canvasRef = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ w: 520, h: 360 });
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
  const hubR = Math.min(size.w, size.h) * 0.1;

  return (
    <div className={`dt-connection-hub ${variant === "hero" ? "dt-connection-hub-hero" : ""}`} aria-label="Connection topology">
      <div className="dt-connection-hub-glow" aria-hidden />
      <div className="dt-connection-hub-canvas" ref={canvasRef}>
        <svg className="dt-connection-hub-svg" width={size.w} height={size.h} viewBox={`0 0 ${size.w} ${size.h}`}>
          <defs>
            <linearGradient id="hub-line-grad" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="#60a5fa" />
              <stop offset="50%" stopColor="#22d3ee" />
              <stop offset="100%" stopColor="#818cf8" />
            </linearGradient>
            <filter id="hub-glow">
              <feGaussianBlur stdDeviation="3" result="blur" />
              <feMerge>
                <feMergeNode in="blur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
            <radialGradient id="hub-core-grad" cx="50%" cy="50%" r="50%">
              <stop offset="0%" stopColor="rgba(96,165,250,0.35)" />
              <stop offset="100%" stopColor="rgba(15,23,42,0.02)" />
            </radialGradient>
          </defs>

          {[0.14, 0.22, 0.3].map((scale, ri) => (
            <ellipse
              key={ri}
              cx={hub.x}
              cy={hub.y}
              rx={hubR * (3.2 + ri * 0.9)}
              ry={hubR * (2.4 + ri * 0.7)}
              className="dt-connection-hub-orbit"
              style={{ animationDelay: `${ri * 0.4}s` }}
            />
          ))}

          {activeNodes.length === 0 && (
            <text x={hub.x} y={hub.y} textAnchor="middle" dominantBaseline="middle" className="dt-connection-hub-empty">
              {emptyHint || "Add connectors to visualize live data flow"}
            </text>
          )}

          {activeNodes.map((node, i) => {
            const { cx, cy, pathId } = layoutNode(i, count);
            const start = toPixel(cx, cy, size.w, size.h);
            const d = curvePath(start.x, start.y, hub.x, hub.y);
            return (
              <g key={node.id}>
                <path id={pathId} d={d} className="dt-connection-hub-path" fill="none" stroke="url(#hub-line-grad)" strokeWidth="2" />
                <circle r="3.5" fill="#38bdf8" filter="url(#hub-glow)">
                  <animateMotion dur={`${2.2 + (i % 4) * 0.3}s`} repeatCount="indefinite">
                    <mpath href={`#${pathId}`} />
                  </animateMotion>
                </circle>
              </g>
            );
          })}

          <circle cx={hub.x} cy={hub.y} r={hubR * 1.35} fill="url(#hub-core-grad)" className="dt-connection-hub-core-glow" />
          <circle cx={hub.x} cy={hub.y} r={hubR} className="dt-connection-hub-core-ring" fill="none" />
        </svg>

        <div className="dt-connection-hub-center" style={{ left: hub.x, top: hub.y }}>
          <div className="dt-connection-hub-center-inner">
            <DtLogo size={Math.round(hubR * 1.1)} />
            <span>{centerLabel}</span>
          </div>
        </div>

        {activeNodes.map((node, i) => {
          const { cx, cy } = layoutNode(i, count);
          const pos = toPixel(cx, cy, size.w, size.h);
          const iconSize = Math.round(Math.min(size.w, size.h) * 0.065);
          return (
            <div
              key={node.id}
              className="dt-connection-hub-node"
              style={{ left: pos.x, top: pos.y, animationDelay: `${i * 0.08}s` }}
              title={node.label}
            >
              <div className="dt-connection-hub-node-glass">
                <ConnectorIcon id={node.type} size={iconSize} />
              </div>
              <span className="dt-connection-hub-node-label">{node.label}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

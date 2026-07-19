import { useEffect, useRef, useState, type CSSProperties } from "react";
import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";

const STAGES = [
  { id: "source", label: "Source", sub: "Any schema" },
  { id: "map", label: "Map", sub: "Semantic" },
  { id: "gates", label: "Preflight", sub: "8 gates" },
  { id: "proof", label: "Proof", sub: "Checksum" },
] as const;

/**
 * Scroll-activated pipeline diagram for the landing platform section.
 * Stages light up in sequence; a particle travels the path when visible.
 */
export function LandingFlowDiagram() {
  const reveal = useRevealOnScroll(0.2);
  const [active, setActive] = useState(0);
  const timerRef = useRef(0);

  useEffect(() => {
    if (!reveal.visible) return;
    const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (prefersReduced) {
      setActive(STAGES.length - 1);
      return;
    }
    setActive(0);
    timerRef.current = window.setInterval(() => {
      setActive((s) => (s + 1) % STAGES.length);
    }, 1600);
    return () => window.clearInterval(timerRef.current);
  }, [reveal.visible]);

  const particleStyle = {
    offsetPath: "path('M72 80 H648')",
    offsetDistance: `${(active / (STAGES.length - 1)) * 100}%`,
  } as CSSProperties;

  return (
    <div
      ref={reveal.ref}
      className={`${reveal.className} lp-flow-diagram`.trim()}
      aria-label="Governed transfer path from source to proof"
    >
      <svg className="lp-flow-diagram-svg" viewBox="0 0 720 160" role="img" aria-hidden>
        <defs>
          <linearGradient id="lp-flow-line" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#99f6e4" />
            <stop offset="50%" stopColor="#0d9488" />
            <stop offset="100%" stopColor="#0f766e" />
          </linearGradient>
          <filter id="lp-flow-glow" x="-20%" y="-40%" width="140%" height="180%">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>
        <path
          className="lp-flow-path"
          d="M72 80 H648"
          fill="none"
          stroke="url(#lp-flow-line)"
          strokeWidth="3"
          strokeLinecap="round"
        />
        <path
          className="lp-flow-path-dash"
          d="M72 80 H648"
          fill="none"
          stroke="#14b8a6"
          strokeWidth="2"
          strokeLinecap="round"
          strokeDasharray="10 14"
        />
        {STAGES.map((stage, i) => {
          const x = 72 + i * 192;
          const on = i <= active;
          return (
            <g key={stage.id} className={`lp-flow-node ${on ? "is-on" : ""}`}>
              <circle cx={x} cy={80} r="22" className="lp-flow-node-ring" />
              <circle
                cx={x}
                cy={80}
                r="14"
                className="lp-flow-node-core"
                filter={on ? "url(#lp-flow-glow)" : undefined}
              />
              <text x={x} y={128} textAnchor="middle" className="lp-flow-node-label">
                {stage.label}
              </text>
              <text x={x} y={146} textAnchor="middle" className="lp-flow-node-sub">
                {stage.sub}
              </text>
            </g>
          );
        })}
        <circle className="lp-flow-particle" r="5" style={particleStyle} />
      </svg>
      <p className="lp-flow-diagram-caption">
        One governed path — Transfer Studio, Pipelines, Pilot, and MCP never skip a gate.
      </p>
    </div>
  );
}

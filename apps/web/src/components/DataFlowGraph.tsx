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

export function DataFlowGraph({ nodes, centerLabel = "DataTransfer", emptyHint }: DataFlowGraphProps) {
  const activeNodes = nodes.filter((n) => n.active !== false);
  const count = Math.max(activeNodes.length, 1);

  return (
    <div className="dt-flow-graph" aria-label="Connected data flow topology">
      <svg className="dt-flow-graph-svg" viewBox="0 0 400 280" preserveAspectRatio="xMidYMid meet">
        <defs>
          <linearGradient id="dt-flow-line" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="var(--dt-flow-start)" />
            <stop offset="100%" stopColor="var(--dt-flow-end)" />
          </linearGradient>
          <filter id="dt-flow-glow">
            <feGaussianBlur stdDeviation="2" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {activeNodes.length === 0 && (
          <text x="200" y="140" textAnchor="middle" className="dt-flow-empty-text">
            {emptyHint || "Connect sources & destinations to see live data flow"}
          </text>
        )}

        {activeNodes.map((node, i) => {
          const angle = (i / count) * Math.PI * 2 - Math.PI / 2;
          const cx = 200 + Math.cos(angle) * 118;
          const cy = 140 + Math.sin(angle) * 88;
          const pathId = `flow-path-${node.id}`;

          return (
            <g key={node.id}>
              <path
                id={pathId}
                d={`M ${cx} ${cy} L 200 140`}
                fill="none"
                stroke="url(#dt-flow-line)"
                strokeWidth="1.5"
                strokeDasharray="6 4"
                className="dt-flow-path"
                opacity={0.55}
              />
              <circle r="3" fill="var(--dt-flow-particle)" filter="url(#dt-flow-glow)">
                <animateMotion dur={`${2.2 + (i % 3) * 0.4}s`} repeatCount="indefinite" rotate="auto">
                  <mpath href={`#${pathId}`} />
                </animateMotion>
              </circle>
              <circle r="2" fill="var(--dt-flow-particle)" opacity={0.7}>
                <animateMotion dur={`${3.1 + (i % 2) * 0.5}s`} repeatCount="indefinite" begin={`${i * 0.3}s`}>
                  <mpath href={`#${pathId}`} />
                </animateMotion>
              </circle>
            </g>
          );
        })}

        <circle cx="200" cy="140" r="36" className="dt-flow-hub-ring" />
        <circle cx="200" cy="140" r="28" className="dt-flow-hub-core" />
      </svg>

      <div className="dt-flow-hub-center">
        <DtLogo size={44} />
        <span>{centerLabel}</span>
      </div>

      <div className="dt-flow-nodes">
        {activeNodes.map((node, i) => {
          const angle = (i / count) * Math.PI * 2 - Math.PI / 2;
          const x = 50 + Math.cos(angle) * 42;
          const y = 50 + Math.sin(angle) * 38;
          return (
            <div
              key={node.id}
              className="dt-flow-node"
              style={{ left: `${x}%`, top: `${y}%` }}
              title={node.label}
            >
              <div className="dt-flow-node-icon">
                <ConnectorIcon id={node.type} size={26} />
              </div>
              <span className="dt-flow-node-label">{node.label}</span>
              <span className="dt-flow-node-pulse" aria-hidden />
            </div>
          );
        })}
      </div>
    </div>
  );
}

import type { GateItem } from "../types";

const STATUS_ICON: Record<GateItem["status"], string> = {
  pass: "✓",
  block: "✕",
  skip: "—",
  pending: "…",
};

const STATUS_CLASS: Record<GateItem["status"], string> = {
  pass: "df-gate-icon--pass",
  block: "df-gate-icon--block",
  skip: "df-gate-icon--skip",
  pending: "df-gate-icon--pending",
};

interface PreflightGateListProps {
  gates: GateItem[];
}

export function PreflightGateList({ gates }: PreflightGateListProps) {
  return (
    <div className="df-card df-card--flat df-gate-panel">
      <div className="df-gate-panel-header">Preflight validation</div>
      <ul className="df-gate-list">
        {gates.map((gate) => (
          <li key={gate.id} className="df-gate-item">
            <span className={["df-gate-icon", STATUS_CLASS[gate.status]].join(" ")}>{STATUS_ICON[gate.status]}</span>
            <div className="df-gate-body">
              <div className="df-gate-label">{gate.label}</div>
              <div className="df-gate-message">{gate.message}</div>
            </div>
            {gate.durationMs != null && (
              <span className="df-gate-duration df-mono">{gate.durationMs.toFixed(1)}ms</span>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}

/** Product gates — matches preflight engine G1–G8 */

export const PREFLIGHT_GATES = [
  { id: "g1", code: "G1", label: "Source ready" },
  { id: "g2", code: "G2", label: "Destination ready" },
  { id: "g3", code: "G3", label: "Schema contract" },
  { id: "g4", code: "G4", label: "Mapping confidence" },
  { id: "g5", code: "G5", label: "Dry-run transform" },
  { id: "g6", code: "G6", label: "Target DDL" },
  { id: "g7", code: "G7", label: "Capacity" },
  { id: "g8", code: "G8", label: "Reconciliation" },
] as const;

interface ProductValueStripProps {
  compact?: boolean;
}

/** Enterprise product line — visible on every transfer surface */
export function ProductValueStrip({ compact = false }: ProductValueStripProps) {
  return (
    <div className={["df-product-strip", compact ? "df-product-strip--compact" : ""].filter(Boolean).join(" ")}>
      <div className="df-product-strip-head">
        <p className="df-product-strip-tag">DataFlow</p>
        <h2 className="df-product-strip-title">
          One-click data transfer with fail-fast preflight
        </h2>
        {!compact && (
          <p className="df-product-strip-desc">
            Any source to any destination. No row moves until all eight gates pass — schema, mapping,
            dry-run, capacity, and reconciliation are validated first.
          </p>
        )}
      </div>
      <div className="df-product-flow" aria-hidden>
        <span className="df-product-flow-node df-product-flow-node--source">Source</span>
        <span className="df-product-flow-line" />
        <span className="df-product-flow-gate">8 gates</span>
        <span className="df-product-flow-line df-product-flow-line--mint" />
        <span className="df-product-flow-node df-product-flow-node--dest">Destination</span>
      </div>
    </div>
  );
}

interface PreflightGateRailProps {
  activeIndex?: number;
  passedCount?: number;
}

export function PreflightGateRail({ activeIndex, passedCount }: PreflightGateRailProps) {
  return (
    <div className="df-gate-rail" role="list" aria-label="Preflight gates">
      {PREFLIGHT_GATES.map((gate, i) => {
        const passed = passedCount !== undefined && i < passedCount;
        const active = activeIndex === i;
        return (
          <div
            key={gate.id}
            role="listitem"
            className={[
              "df-gate-rail-item",
              passed ? "df-gate-rail-item--pass" : "",
              active ? "df-gate-rail-item--active" : "",
            ]
              .filter(Boolean)
              .join(" ")}
            title={gate.label}
          >
            <span className="df-gate-rail-code">{gate.code}</span>
            <span className="df-gate-rail-label">{gate.label}</span>
          </div>
        );
      })}
    </div>
  );
}

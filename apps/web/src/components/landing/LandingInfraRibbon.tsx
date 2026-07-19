import { useEffect, useRef, useState, type CSSProperties } from "react";
import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";

const NODES = [
  { id: "db", label: "OLTP", sub: "PostgreSQL", kind: "server" as const },
  { id: "map", label: "Map", sub: "Semantic", kind: "glass" as const },
  { id: "gate", label: "Gates", sub: "8 preflight", kind: "glass" as const },
  { id: "wh", label: "Warehouse", sub: "Snowflake", kind: "server" as const },
];

/**
 * Full-bleed mid-page infrastructure ribbon — isometric nodes with live pulse.
 */
export function LandingInfraRibbon() {
  const reveal = useRevealOnScroll(0.15);
  const [active, setActive] = useState(0);
  const timer = useRef(0);

  useEffect(() => {
    if (!reveal.visible) return;
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      setActive(NODES.length - 1);
      return;
    }
    timer.current = window.setInterval(() => {
      setActive((s) => (s + 1) % NODES.length);
    }, 1400);
    return () => window.clearInterval(timer.current);
  }, [reveal.visible]);

  return (
    <div
      ref={reveal.ref}
      className={`lp-infra ${reveal.className}`.trim()}
      aria-label="Governed path across real systems"
    >
      <div className="lp-infra-stage lp-infra-stage--flat">
        <div className="lp-infra-floor" aria-hidden />
        <div className="lp-infra-beam" aria-hidden style={{ "--active": active } as CSSProperties} />
        {NODES.map((node, i) => (
          <article
            key={node.id}
            className={`lp-infra-node lp-infra-node--${node.kind} ${i <= active ? "is-on" : ""}`}
            style={{ "--i": i } as CSSProperties}
          >
            <div className="lp-infra-node-3d">
              <span className="lp-infra-face lp-infra-face--front">
                {node.kind === "server" ? (
                  <span className="lp-infra-bays">
                    <i /><i /><i /><i />
                  </span>
                ) : (
                  <span className="lp-infra-chip">{String(i + 1).padStart(2, "0")}</span>
                )}
              </span>
              <span className="lp-infra-face lp-infra-face--top" />
              <span className="lp-infra-face lp-infra-face--side" />
            </div>
            <strong>{node.label}</strong>
            <small>{node.sub}</small>
          </article>
        ))}
        <div className="lp-infra-packet" style={{ "--active": active } as CSSProperties} aria-hidden />
      </div>
      <p className="lp-infra-caption">
        Real systems on both ends — DataFlow sits in the middle with mapping, gates, and proof.
      </p>
    </div>
  );
}

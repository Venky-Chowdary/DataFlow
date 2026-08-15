import { useState } from "react";
import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";

const STEPS = [
  {
    id: "map",
    kicker: "01 · Map",
    title: "Semantic map, not string match",
    body: "Every source column is scored against the destination. Extra source fields stay visible. Dest-only NOT NULL blocks create. False friends stay in review until you confirm them.",
    bars: [
      { label: "order_id → order_id", width: 96, tone: "ok" },
      { label: "order_qty ↛ order_amt", width: 42, tone: "warn" },
      { label: "loyalty_tier (extra)", width: 70, tone: "info" },
    ],
  },
  {
    id: "validate",
    kicker: "02 · Validate",
    title: "Eight gates before a row moves",
    body: "Preflight fails fast on type, PK, cursor, and dest-exists shape. skip_preflight never comes from chat or the public Studio execute path.",
    bars: [
      { label: "G1–G8 pass", width: 88, tone: "ok" },
      { label: "G14 dest NOT NULL", width: 54, tone: "warn" },
      { label: "G15 shape named", width: 76, tone: "info" },
    ],
  },
  {
    id: "write",
    kicker: "03 · Write",
    title: "Quarantine, never silent drop",
    body: "Bad rows surface with a reason. Procedure CALL failures quarantine. CDC default is at-least-once upsert until a route proves dest-owned exactly-once.",
    bars: [
      { label: "Applied", width: 82, tone: "ok" },
      { label: "Quarantine", width: 18, tone: "warn" },
      { label: "Dropped silently", width: 0, tone: "muted" },
    ],
  },
  {
    id: "prove",
    kicker: "04 · Prove",
    title: "Checksum reconcile on the dest engine",
    body: "Theater shows dest LSN, fence, quarantine, and dest-engine COUNT — never a writer-stamped scan().count(). If a job finished, you can show the proof.",
    bars: [
      { label: "Source checksum", width: 90, tone: "info" },
      { label: "Dest checksum", width: 90, tone: "ok" },
      { label: "Lag (commit_ts)", width: 34, tone: "warn" },
    ],
  },
] as const;

export function ObservabilityInAction() {
  const reveal = useRevealOnScroll();
  const [active, setActive] = useState(0);
  const step = STEPS[active] ?? STEPS[0];

  return (
    <section
      className="lp-obs"
      id="observability-in-action"
      aria-label="See observability in action"
    >
      <div ref={reveal.ref} className={`lp-obs-inner ${reveal.className}`}>
        <header className="lp-home-section-head">
          <p className="lp-section-kicker">See observability in action</p>
          <h2>The journey is the product.</h2>
          <p>
            DataKitchen-class journey thinking, Datawrap proof: Map → Validate → Write → Prove.
            Each step is the same engine Studio, Pipelines, Pilot, and MCP already run — not a
            marketing mock.
          </p>
        </header>

        <div className="lp-obs-stage">
          <ol className="lp-obs-steps" role="tablist" aria-label="Transfer journey">
            {STEPS.map((s, i) => (
              <li key={s.id}>
                <button
                  type="button"
                  role="tab"
                  aria-selected={i === active}
                  className={`lp-obs-step${i === active ? " is-active" : ""}`}
                  onClick={() => setActive(i)}
                >
                  <span className="lp-obs-step-kicker">{s.kicker}</span>
                  <strong>{s.title}</strong>
                </button>
              </li>
            ))}
          </ol>

          <div className="lp-obs-panel" role="tabpanel">
            <div className="lp-obs-copy">
              <p className="lp-obs-kicker">{step.kicker}</p>
              <h3>{step.title}</h3>
              <p>{step.body}</p>
            </div>
            <figure className="lp-obs-chart" aria-label={`${step.title} proof chart`}>
              <figcaption>Operator proof — measured bars, not a stock graphic</figcaption>
              <ul>
                {step.bars.map((bar) => (
                  <li key={bar.label} className={`lp-obs-bar is-${bar.tone}`}>
                    <span className="lp-obs-bar-label">{bar.label}</span>
                    <span className="lp-obs-bar-track">
                      <span
                        className="lp-obs-bar-fill"
                        style={{ width: `${bar.width}%` }}
                      />
                    </span>
                    <span className="lp-obs-bar-val">{bar.width}%</span>
                  </li>
                ))}
              </ul>
            </figure>
          </div>
        </div>
      </div>
    </section>
  );
}

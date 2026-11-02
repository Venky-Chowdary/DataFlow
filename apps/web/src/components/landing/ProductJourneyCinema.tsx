import { useEffect, useState } from "react";
import { REAL_PREFLIGHT_GATES } from "../../pages/marketing/productPageShared";

export type JourneyStepId = "map" | "validate" | "write" | "prove";

const STEPS: {
  id: JourneyStepId;
  kicker: string;
  title: string;
  body: string;
}[] = [
  {
    id: "map",
    kicker: "01 · Map",
    title: "Semantic map by meaning",
    body: "Every source column is scored against the destination by role, qualifier, and type. Extra source fields stay visible, and look-alike columns wait for your confirmation.",
  },
  {
    id: "validate",
    kicker: "02 · Validate",
    title: "Nine gates before a row moves",
    body: "Preflight checks types, keys, collation, cursor, privileges, and the destination's shape before the write — from Studio, schedules, chat, and agents alike.",
  },
  {
    id: "write",
    kicker: "03 · Write",
    title: "Quarantine with a reason",
    body: "Held-back rows surface with column, value, and reason, and replay once fixed. Stored-procedure and destination INSERT/MERGE writes quarantine the same way.",
  },
  {
    id: "prove",
    kicker: "04 · Prove",
    title: "Verified on the destination engine",
    body: "Gate-8 reads the destination back: engine COUNT, a value and key census, and quarantine counts in Job Theater. Every finished job carries its proof.",
  },
];

const MAP_ROWS: { src: string; dest: string; score: string; tone: "ok" | "warn" | "info" }[] = [
  { src: "order_amt", dest: "total_amount", score: "0.92", tone: "ok" },
  { src: "pay_amt", dest: "payment_amount", score: "0.99", tone: "ok" },
  { src: "cust_id", dest: "customer_key", score: "review", tone: "warn" },
  { src: "loyalty_tier", dest: "(extra source)", score: "G13", tone: "info" },
];

interface ProductJourneyCinemaProps {
  compact?: boolean;
  autoPlay?: boolean;
}

/**
 * Clickable Map → Validate → Write → Prove stage.
 * One owner for landing + solutions heroes so ink-band inheritance cannot
 * bleach the semantic map again.
 */
export function ProductJourneyCinema({ compact = false, autoPlay = true }: ProductJourneyCinemaProps) {
  const [active, setActive] = useState(0);
  const [paused, setPaused] = useState(false);
  const step = STEPS[active] ?? STEPS[0];

  useEffect(() => {
    if (!autoPlay || paused) return;
    const id = window.setInterval(() => setActive((i) => (i + 1) % STEPS.length), 2800);
    return () => window.clearInterval(id);
  }, [autoPlay, paused]);

  return (
    <div className={`lp-journey${compact ? " is-compact" : ""}`}>
      <ol className="lp-journey-tabs" role="tablist" aria-label="Transfer journey">
        {STEPS.map((s, i) => (
          <li key={s.id}>
            <button
              type="button"
              role="tab"
              aria-selected={i === active}
              className={`lp-journey-tab${i === active ? " is-active" : ""}`}
              onClick={() => {
                setActive(i);
                setPaused(true);
              }}
            >
              <span>{s.kicker}</span>
              <strong>{compact ? s.id : s.title}</strong>
            </button>
          </li>
        ))}
      </ol>

      <div className="lp-journey-body" role="tabpanel">
        {compact ? null : (
          <div className="lp-journey-copy">
            <p className="lp-journey-kicker">{step.kicker}</p>
            <h3>{step.title}</h3>
            <p>{step.body}</p>
            <button
              type="button"
              className="lp-journey-play"
              onClick={() => setPaused((p) => !p)}
            >
              {paused ? "Play journey" : "Pause"}
            </button>
          </div>
        )}
        <JourneyStage stage={step.id} gateTick={active === 1 ? 8 : 9} />
      </div>
    </div>
  );
}

function JourneyStage({ stage, gateTick }: { stage: JourneyStepId; gateTick: number }) {
  if (stage === "map") {
    return (
      <div className="lp-journey-shot" aria-label="Semantic map">
        <header>
          <h4>Semantic map</h4>
          <em>4 fields · 1 review</em>
        </header>
        {MAP_ROWS.map((row) => (
          <div key={row.src} className={`lp-journey-map-row is-${row.tone}`}>
            <code>{row.src}</code>
            <span aria-hidden>→</span>
            <code>{row.dest}</code>
            <em>{row.score}</em>
          </div>
        ))}
      </div>
    );
  }
  if (stage === "validate") {
    const shown = REAL_PREFLIGHT_GATES.slice(0, compactSafe(gateTick));
    return (
      <div className="lp-journey-shot" aria-label="Preflight G1–G9">
        <header>
          <h4>Preflight · {shown.length}/9</h4>
          <em>fail-fast</em>
        </header>
        <div className="lp-journey-progress" aria-hidden>
          <i style={{ width: `${(shown.length / 9) * 100}%` }} />
        </div>
        <ul className="lp-journey-gates">
          {REAL_PREFLIGHT_GATES.map((g, i) => (
            <li key={g.id} className={i < shown.length ? "is-pass" : "is-pending"}>
              <span>{g.id} {g.title}</span>
              <em>{i < shown.length ? "pass" : "…"}</em>
            </li>
          ))}
        </ul>
      </div>
    );
  }
  if (stage === "write") {
    return (
      <div className="lp-journey-shot" aria-label="Write with quarantine">
        <header>
          <h4>Write · upsert</h4>
          <em>idempotent</em>
        </header>
        <div className="lp-journey-metrics">
          <div><strong>12,471</strong><span>Applied</span></div>
          <div className="is-warn"><strong>9</strong><span>Quarantine</span></div>
          <div className="is-muted"><strong>0</strong><span>Silent drop</span></div>
        </div>
        <p className="lp-journey-note">
          <code>order_amt</code> “$1,204.00” → NUMERIC failed. Row isolated with reason — replayable.
        </p>
      </div>
    );
  }
  return (
    <div className="lp-journey-shot" aria-label="Checksum proof">
      <header>
        <h4>Reconcile</h4>
        <em className="is-ok">MATCH</em>
      </header>
      <div className="lp-journey-metrics">
        <div><strong>12,480</strong><span>Source</span></div>
        <div><strong>12,471</strong><span>Dest COUNT</span></div>
        <div className="is-ok"><strong>MATCH</strong><span>Checksum</span></div>
      </div>
      <p className="lp-journey-note">
        Destination-engine COUNT plus a value and key census — the 9 held-back rows wait in quarantine with reasons.
      </p>
    </div>
  );
}

function compactSafe(n: number): number {
  return Math.max(1, Math.min(9, n));
}

export function TransferStudioHeroShot() {
  return (
    <figure className="lp-mkt-shot lp-mkt-shot--light">
      <div className="lp-mkt-shot-chrome">
        <span className="lp-mkt-shot-dots" aria-hidden>
          <i /><i /><i />
        </span>
        <span className="lp-mkt-shot-label">Transfer Studio · Orders migration</span>
      </div>
      <div className="lp-mkt-shot-body">
        <ProductJourneyCinema compact autoPlay />
      </div>
      <figcaption>
        Click Map → Validate → Write → Prove. Semantic scores stay ink-on-white — the same path Studio runs.
      </figcaption>
    </figure>
  );
}

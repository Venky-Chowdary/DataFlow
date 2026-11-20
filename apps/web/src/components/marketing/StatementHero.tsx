import { useEffect, useMemo, useState } from "react";

/**
 * The home page's one self-starting moment: an example run's statement,
 * read = landed + held + skipped, counted up once and sealed when it
 * balances. Every figure adds up. Under reduced motion it renders settled.
 */

export const EXAMPLE_STATEMENT = {
  source: "public.sales_orders",
  sourceEngine: "PostgreSQL",
  destination: "DW_DEMO.SALES_ORDERS_REPORTING",
  destinationEngine: "Snowflake",
  landed: 48_213_871,
  held: 33,
  skipped: 2,
} as const;

export const EXAMPLE_READ =
  EXAMPLE_STATEMENT.landed + EXAMPLE_STATEMENT.held + EXAMPLE_STATEMENT.skipped;

const fmt = new Intl.NumberFormat("en-US");
const DURATION_MS = 1600;

function prefersReducedMotion(): boolean {
  try {
    return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  } catch {
    return true;
  }
}

/** 0 → 1 once over DURATION_MS, eased; 1 at once under reduced motion. */
function useCountUp(): number {
  const [t, setT] = useState(() => (prefersReducedMotion() ? 1 : 0));
  useEffect(() => {
    if (t >= 1) return;
    let raf = 0;
    const start = performance.now();
    const tick = (now: number) => {
      const p = Math.min(1, (now - start) / DURATION_MS);
      setT(1 - Math.pow(1 - p, 3));
      if (p < 1) raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
    // Runs once: the statement counts up a single time per visit.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  return t;
}

/** Points of a hypotrochoid, the curve family security printing calls guilloché. */
function trochoid(R: number, r: number, d: number, turns: number, steps: number, cx: number, cy: number) {
  const pts: string[] = [];
  for (let i = 0; i <= steps; i++) {
    const a = (i / steps) * Math.PI * 2 * turns;
    const x = cx + (R - r) * Math.cos(a) + d * Math.cos(((R - r) / r) * a);
    const y = cy + (R - r) * Math.sin(a) - d * Math.sin(((R - r) / r) * a);
    pts.push(`${x.toFixed(2)},${y.toFixed(2)}`);
  }
  return `M${pts.join("L")}`;
}

/** The seal: three interleaved guilloché rings around a check mark. */
export function GuillocheSeal({ drawn, label }: { drawn: boolean; label?: string }) {
  const paths = useMemo(
    () => [
      trochoid(56, 7, 15, 7, 1400, 60, 60),
      trochoid(48, 6, 11, 6, 1200, 60, 60),
      trochoid(40, 5, 8, 5, 1000, 60, 60),
    ],
    [],
  );
  return (
    <svg
      className={`mk-seal${drawn ? " is-drawn" : ""}`}
      viewBox="0 0 120 120"
      {...(label ? { role: "img", "aria-label": label } : { "aria-hidden": true })}
    >
      {paths.map((d, i) => (
        <path key={i} d={d} pathLength={1} className={`mk-seal-ring mk-seal-ring--${i}`} />
      ))}
      <circle cx="60" cy="60" r="22" className="mk-seal-core" />
      <path d="M50 60.5l7 7 14-15" pathLength={1} className="mk-seal-check" />
    </svg>
  );
}

/**
 * An identifier that may wrap only after "." or "_" (DW_DEMO.SALES_ORDERS_
 * REPORTING), never mid-word, on a narrow screen.
 */
export function Breakable({ id }: { id: string }) {
  const parts = id.split(/(?<=[._])/);
  return (
    <>
      {parts.map((p, i) => (
        <span key={i}>
          {p}
          {i < parts.length - 1 ? <wbr /> : null}
        </span>
      ))}
    </>
  );
}

export function StatementHero() {
  const t = useCountUp();
  const done = t >= 1;
  const s = EXAMPLE_STATEMENT;
  const read = Math.round(EXAMPLE_READ * t);
  const landed = Math.round(s.landed * t);
  const held = Math.round(s.held * t);
  const skipped = Math.round(s.skipped * t);

  return (
    <figure className="mk-statement" aria-labelledby="mk-statement-title">
      <figcaption className="mk-statement-head">
        <span id="mk-statement-title" className="mk-statement-title">
          Example statement for one run
        </span>
        <span className="mk-statement-route">
          <code>
            <Breakable id={s.source} />
          </code>{" "}
          in {s.sourceEngine} into{" "}
          <code>
            <Breakable id={s.destination} />
          </code>{" "}
          in {s.destinationEngine}
        </span>
      </figcaption>

      <p className="mk-sr-only">
        {fmt.format(EXAMPLE_READ)} rows read equals {fmt.format(s.landed)} landed, plus {s.held} held, plus{" "}
        {s.skipped} skipped. The statement balances.
      </p>

      <div className="mk-statement-body" aria-hidden>
        <dl className="mk-equation">
          <div className="mk-term mk-term--read">
            <dt>read</dt>
            <dd>{fmt.format(read)}</dd>
          </div>
          <span className="mk-op mk-op--1">=</span>
          <div className="mk-term mk-term--landed">
            <dt>landed</dt>
            <dd>{fmt.format(landed)}</dd>
          </div>
          <span className="mk-op mk-op--2">+</span>
          <div className="mk-term mk-term--held">
            <dt>held</dt>
            <dd>{fmt.format(held)}</dd>
          </div>
          <span className="mk-op mk-op--3">+</span>
          <div className="mk-term mk-term--skipped">
            <dt>skipped</dt>
            <dd>{fmt.format(skipped)}</dd>
          </div>
        </dl>
        <div className={`mk-statement-seal${done ? " is-done" : ""}`}>
          <GuillocheSeal drawn={done} />
          <span className="mk-statement-state">{done ? "Balanced" : "Counting the destination"}</span>
        </div>
      </div>

      <ul className="mk-statement-lines">
        <li className="mk-line mk-line--held">
          <span className="mk-line-count">33 held</span>
          <span className="mk-line-why">
            <code>status</code> is NULL, and the destination column is NOT NULL. Each row is kept with its
            value and reason, ready to replay once the column is fixed.
          </span>
        </li>
        <li className="mk-line mk-line--skipped">
          <span className="mk-line-count">2 skipped</span>
          <span className="mk-line-why">
            <code>region = 'TEST'</code>, excluded by the filter on this run.
          </span>
        </li>
        <li className="mk-line mk-line--landed">
          <span className="mk-line-count">Landed rows</span>
          <span className="mk-line-why">Counted by Snowflake itself after the write, then compared with the source.</span>
        </li>
      </ul>
    </figure>
  );
}

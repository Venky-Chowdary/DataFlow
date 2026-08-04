/**
 * AlgorithmCinema — product-feeling animated stages that carry the
 * Datawrap story on marketing pages. These are NOT decorative dots;
 * each stage renders a concrete slice of the real engine: semantic
 * mapping, G1–G8 preflight, checksum proof, and CDC handoff.
 *
 * Guardrails:
 *  - All motion honors prefers-reduced-motion (final state is rendered).
 *  - No external animation libraries — plain React state + setInterval.
 *  - Class names are `.lp-cinema-*` so CSS lives with landing.css bands.
 */
import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type ReactNode,
} from "react";
import { useInView } from "../../hooks/useInView";

/* ─── Shared helpers ─────────────────────────────────────────────── */

function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);
  useEffect(() => {
    if (typeof window === "undefined") return;
    const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
    const apply = () => setReduced(mq.matches);
    apply();
    mq.addEventListener?.("change", apply);
    return () => mq.removeEventListener?.("change", apply);
  }, []);
  return reduced;
}

/** Cycle only while the stage is on-screen — prevents landing scroll lag. */
function useVisibleCycle(length: number, intervalMs: number, active: boolean): number {
  const reduced = usePrefersReducedMotion();
  const [i, setI] = useState(reduced ? Math.max(0, length - 1) : 0);
  useEffect(() => {
    if (reduced) {
      setI(Math.max(0, length - 1));
      return;
    }
    if (!active) return;
    const id = window.setInterval(() => setI((v) => (v + 1) % length), intervalMs);
    return () => window.clearInterval(id);
  }, [length, intervalMs, reduced, active]);
  return i;
}

/* ─── 1) MappingCinema ───────────────────────────────────────────── */

interface MappingEdge {
  source: string;
  target: string;
  confidence: number;
  role: string;
}

const MAPPING_EDGES: MappingEdge[] = [
  { source: "order_amt", target: "payment_amount", confidence: 0.96, role: "amount" },
  { source: "cust_email", target: "email", confidence: 0.94, role: "email" },
  { source: "order_id", target: "order_key", confidence: 0.91, role: "identifier" },
  { source: "created_at", target: "created_at", confidence: 0.99, role: "timestamp" },
];

export function MappingCinema() {
  const { ref, inView } = useInView<HTMLElement>("80px 0px");
  const tick = useVisibleCycle(7, 900, inView);

  const drawn = Math.min(MAPPING_EDGES.length, tick + 1);
  const reviewIndex = 2;
  const reviewActive = tick === 3;
  const pinned = tick >= 4;

  return (
    <figure ref={ref} className="lp-cinema-stage lp-cinema-mapping" aria-label="Semantic mapping animation">
      <div className="lp-cinema-mapping-grid">
        <div className="lp-cinema-col" aria-label="Source columns">
          <span className="lp-cinema-col-head">Source · orders.csv</span>
          {MAPPING_EDGES.map((edge, i) => (
            <div
              key={`s-${edge.source}`}
              className={`lp-cinema-field${i < drawn ? " is-live" : ""}`}
              style={{ "--i": i } as CSSProperties}
            >
              <code>{edge.source}</code>
              <em>{edge.role}</em>
            </div>
          ))}
        </div>

        <svg
          className="lp-cinema-wires"
          viewBox="0 0 200 260"
          preserveAspectRatio="none"
          aria-hidden
        >
          {MAPPING_EDGES.map((edge, i) => {
            const y = 26 + i * 60;
            const state =
              i >= drawn
                ? "pending"
                : i === reviewIndex && reviewActive
                  ? "review"
                  : i === reviewIndex && !pinned && tick >= 2
                    ? "draw"
                    : i < drawn
                      ? "pin"
                      : "draw";
            return (
              <g key={`w-${edge.source}`} className={`lp-cinema-wire is-${state}`}>
                <path d={`M0,${y} C80,${y} 120,${y} 200,${y}`} />
                <rect
                  className="lp-cinema-wire-score-bg"
                  x="78"
                  y={y - 22}
                  width="44"
                  height="18"
                  rx="5"
                />
                <text x="100" y={y - 9} textAnchor="middle" className="lp-cinema-wire-score">
                  {edge.confidence.toFixed(2)}
                </text>
              </g>
            );
          })}
        </svg>

        <div className="lp-cinema-col lp-cinema-col--right" aria-label="Destination columns">
          <span className="lp-cinema-col-head">Destination · payments</span>
          {MAPPING_EDGES.map((edge, i) => (
            <div
              key={`t-${edge.target}`}
              className={`lp-cinema-field${i < drawn ? " is-live" : ""}${
                i === reviewIndex && reviewActive ? " is-review" : ""
              }`}
              style={{ "--i": i } as CSSProperties}
            >
              <code>{edge.target}</code>
              <em>{edge.role}</em>
            </div>
          ))}
        </div>
      </div>

      <div className="lp-cinema-status" role="status" aria-live="polite">
        {reviewActive ? (
          <>
            <span className="lp-cinema-chip is-warn">review</span>
            <span>
              <code>order_id → order_key</code> · role match, name divergent — human confirms.
            </span>
          </>
        ) : pinned ? (
          <>
            <span className="lp-cinema-chip is-ok">pinned</span>
            <span>
              <code>order_id → order_key</code> added to workspace synonyms.
            </span>
          </>
        ) : (
          <>
            <span className="lp-cinema-chip">scoring</span>
            <span>Format · role · type — each edge earns a continuous confidence.</span>
          </>
        )}
      </div>

      <figcaption>
        Semantic mapping — roles, synonyms, type fit — not string equality.
      </figcaption>
    </figure>
  );
}

/* ─── 2) GateCinema ──────────────────────────────────────────────── */

/**
 * Local copy of the 8 preflight gate titles. We intentionally do NOT
 * import from `pages/marketing/productPageShared` to avoid pulling a
 * page-tree module into the landing component tree (circular risk).
 * The engine truth still lives in `packages/preflight` — this list is
 * a marketing mirror kept in sync with `REAL_PREFLIGHT_GATES`.
 */
const GATE_TITLES: { id: string; title: string; algorithm: string }[] = [
  { id: "G1", title: "Source", algorithm: "Parse headers · require ≥1 column" },
  { id: "G2", title: "Destination", algorithm: "Reachability · write privileges" },
  { id: "G3", title: "Schema contract", algorithm: "Type family · nullability · precision" },
  { id: "G4", title: "Mapping confidence", algorithm: "Edge score ≥ workspace threshold" },
  { id: "G5", title: "Dry-run", algorithm: "Coerce sample through real transforms" },
  { id: "G6", title: "Target DDL", algorithm: "Write plan valid against target object" },
  { id: "G7", title: "Capacity", algorithm: "Estimate vs destination limits" },
  { id: "G8", title: "Reconciliation plan", algorithm: "Row count + checksum strategy ready" },
];

export function GateCinema() {
  const { ref, inView } = useInView<HTMLElement>("80px 0px");
  const tick = useVisibleCycle(10, 1100, inView);

  const active = Math.min(tick, GATE_TITLES.length - 1);
  const g5Active = active === 4;
  // Quarantine panel latches while G5 is scrutinised and stays visible
  // through G6 for continuity, then fades as later gates run.
  const showQuarantine = active >= 4;

  return (
    <figure ref={ref} className="lp-cinema-stage lp-cinema-gate" aria-label="Preflight gates animation">
      <div className="lp-cinema-gate-grid">
        <ol className="lp-cinema-gate-list" aria-label="Preflight G1–G8">
          {GATE_TITLES.map((g, i) => {
            const state =
              i < active ? "pass" : i === active ? (g5Active ? "block" : "active") : "pending";
            return (
              <li key={g.id} className={`lp-cinema-gate-row is-${state}`}>
                <span className="lp-cinema-gate-id">{g.id}</span>
                <span className="lp-cinema-gate-body">
                  <strong>{g.title}</strong>
                  <em>{g.algorithm}</em>
                </span>
                <span className="lp-cinema-gate-status" aria-hidden>
                  {state === "pass"
                    ? "✓"
                    : state === "active"
                      ? "…"
                      : state === "block"
                        ? "!"
                        : ""}
                </span>
              </li>
            );
          })}
        </ol>

        <aside
          className={`lp-cinema-quarantine${showQuarantine ? " is-live" : ""}`}
          aria-label="Quarantine sample"
        >
          <header>
            <span className="lp-cinema-chip is-warn">G5 · dry-run</span>
            <h4>Coerce fail → quarantine</h4>
          </header>
          <div className="lp-cinema-quarantine-row">
            <span className="lp-cinema-cell-label">order_amt (STRING)</span>
            <code className="lp-cinema-cell-value">&quot;$1,204.00&quot;</code>
            <span className="lp-cinema-arrow" aria-hidden>→</span>
            <span className="lp-cinema-cell-label">payment_amount (NUMERIC)</span>
          </div>
          <p className="lp-cinema-quarantine-reason">
            <strong>Reason:</strong> currency symbol prevents lossless NUMERIC coerce.
            Row isolates to quarantine with column + value + reason — never silently dropped.
          </p>
          <div className={`lp-cinema-quarantine-final${active >= 7 ? " is-visible" : ""}`}>
            <span className="lp-cinema-chip is-ok">G8</span>
            <span>Reconcile plan ready — checksum + row-count strategy pinned.</span>
          </div>
        </aside>
      </div>

      <figcaption>
        Eight gates from the real engine — one <em>block</em> stops write. No “best-effort” drift.
      </figcaption>
    </figure>
  );
}

/* ─── 3) ProofCinema ─────────────────────────────────────────────── */

interface QuarantineSample {
  column: string;
  value: string;
  reason: string;
}

const PROOF_QUARANTINE: QuarantineSample[] = [
  { column: "payment_amount", value: "\"$1,204.00\"", reason: "currency symbol · NUMERIC coerce" },
  { column: "email", value: "\"n/a\"", reason: "sentinel string · email role required" },
];

function useCountUp(target: number, start: boolean, durationMs = 1100): number {
  const reduced = usePrefersReducedMotion();
  const [value, setValue] = useState(reduced ? target : 0);
  const rafRef = useRef<number>(0);
  useEffect(() => {
    if (reduced || !start) {
      setValue(target);
      return;
    }
    const t0 = performance.now();
    const tick = (now: number) => {
      const t = Math.min(1, (now - t0) / durationMs);
      const eased = 1 - Math.pow(1 - t, 3);
      setValue(Math.round(eased * target));
      if (t < 1) rafRef.current = window.requestAnimationFrame(tick);
    };
    rafRef.current = window.requestAnimationFrame(tick);
    return () => window.cancelAnimationFrame(rafRef.current);
  }, [target, start, durationMs, reduced]);
  return value;
}

export function ProofCinema() {
  const { ref, inView } = useInView<HTMLElement>("80px 0px");
  const tick = useVisibleCycle(4, 1600, inView);

  const source = useCountUp(12480, inView && tick >= 0);
  const written = useCountUp(12471, inView && tick >= 0);
  const quarantined = useCountUp(9, inView && tick >= 0);
  const showChecksum = tick >= 1;
  const match = tick >= 2;

  return (
    <figure ref={ref} className="lp-cinema-stage lp-cinema-proof" aria-label="Reconciliation proof animation">
      <div className="lp-cinema-proof-grid">
        <div className="lp-cinema-proof-panel">
          <span className="lp-cinema-panel-label">Source (mapped)</span>
          <code className={`lp-cinema-hash${showChecksum ? " is-visible" : ""}`}>
            sha256:a3f1c9e1…
          </code>
          <div className="lp-cinema-metric">
            <strong>{source.toLocaleString()}</strong>
            <span>rows</span>
          </div>
        </div>

        <div
          className={`lp-cinema-proof-match${match ? " is-match" : ""}`}
          role="status"
          aria-live="polite"
        >
          <span className="lp-cinema-match-badge">{match ? "MATCH" : "reconcile"}</span>
          <span className="lp-cinema-match-detail">
            {match ? "checksum + counts verified" : "hashing mapped rows"}
          </span>
        </div>

        <div className="lp-cinema-proof-panel">
          <span className="lp-cinema-panel-label">Destination</span>
          <code className={`lp-cinema-hash${showChecksum ? " is-visible" : ""}`}>
            sha256:a3f1c9e1…
          </code>
          <div className="lp-cinema-metric">
            <strong>{written.toLocaleString()}</strong>
            <span>rows written</span>
          </div>
          <div className="lp-cinema-metric lp-cinema-metric--quarantine">
            <strong>{quarantined}</strong>
            <span>quarantined</span>
          </div>
        </div>
      </div>

      <div className="lp-cinema-proof-quarantine">
        <div className="lp-cinema-proof-quarantine-head">
          <span className="lp-cinema-chip is-warn">quarantine</span>
          <span>Isolated with column + value + reason — never silently dropped.</span>
        </div>
        <ul>
          {PROOF_QUARANTINE.map((row) => (
            <li key={row.column}>
              <code>{row.column}</code>
              <span className="lp-cinema-quarantine-val">{row.value}</span>
              <em>{row.reason}</em>
            </li>
          ))}
        </ul>
      </div>

      <figcaption>
        Checksum + row-count reconcile after every write — matched or failed, never assumed.
      </figcaption>
    </figure>
  );
}

/* ─── 4) CdcCinema ───────────────────────────────────────────────── */

export function CdcCinema() {
  const { ref, inView } = useInView<HTMLElement>("80px 0px");
  const tick = useVisibleCycle(8, 1000, inView);

  const snapshotProgress = Math.min(1, tick / 2);
  const handoff = tick >= 2;
  const streamTicks = handoff ? tick - 1 : 0;

  return (
    <figure ref={ref} className="lp-cinema-stage lp-cinema-cdc" aria-label="CDC snapshot + streaming animation">
      <div className="lp-cinema-cdc-timeline" aria-hidden>
        <div className="lp-cinema-cdc-phase lp-cinema-cdc-phase--snap">
          <div className="lp-cinema-cdc-phase-head">
            <strong>Snapshot window</strong>
            <em>upsert on PK</em>
          </div>
          <div className="lp-cinema-cdc-bar">
            <i style={{ width: `${snapshotProgress * 100}%` }} />
          </div>
        </div>

        <div className={`lp-cinema-cdc-handoff${handoff ? " is-live" : ""}`}>
          <span className="lp-cinema-cdc-lsn">LSN 0/16A2B40</span>
          <span className="lp-cinema-cdc-arrow" aria-hidden>→</span>
        </div>

        <div className="lp-cinema-cdc-phase lp-cinema-cdc-phase--stream">
          <div className="lp-cinema-cdc-phase-head">
            <strong>Streaming upserts</strong>
            <em>{handoff ? "at-least-once" : "idle"}</em>
          </div>
          <ol className="lp-cinema-cdc-ticks">
            {Array.from({ length: 6 }, (_, i) => (
              <li
                key={i}
                className={`lp-cinema-cdc-tick${i < streamTicks ? " is-live" : ""}`}
                style={{ "--i": i } as CSSProperties}
              />
            ))}
          </ol>
        </div>
      </div>

      <div className="lp-cinema-cdc-legend" role="list">
        <span role="listitem" className="lp-cinema-chip">snapshot + LSN handoff</span>
        <span role="listitem" className="lp-cinema-chip is-ok">idempotent upsert on PK</span>
        <span role="listitem" className="lp-cinema-chip is-warn">
          at-least-once until exactly-once is proven
        </span>
      </div>

      <figcaption>
        Snapshot + LSN handoff — honest CDC default. Streaming upserts idempotent on primary key.
      </figcaption>
    </figure>
  );
}

/* ─── 5) AlgorithmCinemaBand — wrapper ───────────────────────────── */

export interface AlgorithmCinemaBandProps {
  kicker?: string;
  title: string;
  lead?: string;
  children: ReactNode;
  cta?: ReactNode;
  /** Optional side-copy chapters to sit alongside the stage on wide viewports. */
  aside?: ReactNode;
  /** Reduce vertical padding when band is chained back-to-back. */
  compact?: boolean;
  id?: string;
}

export function AlgorithmCinemaBand({
  kicker,
  title,
  lead,
  children,
  cta,
  aside,
  compact,
  id,
}: AlgorithmCinemaBandProps) {
  return (
    <section
      id={id}
      className={`lp-cinema-band${compact ? " lp-cinema-band--compact" : ""}`}
      aria-label={title}
    >
      <div className="lp-cinema-band-inner">
        <header className="lp-cinema-band-head">
          {kicker ? <p className="lp-cinema-band-kicker">{kicker}</p> : null}
          <h2>{title}</h2>
          {lead ? <p className="lp-cinema-band-lead">{lead}</p> : null}
        </header>
        <div className={`lp-cinema-band-body${aside ? " has-aside" : ""}`}>
          <div className="lp-cinema-band-stage">{children}</div>
          {aside ? <div className="lp-cinema-band-aside">{aside}</div> : null}
        </div>
        {cta ? <div className="lp-cinema-band-cta">{cta}</div> : null}
      </div>
    </section>
  );
}

/* ─── 6) ProductSurfaceStrip ─────────────────────────────────────── */

export interface ProductSurfaceStripProps<Route extends string> {
  onNavigate: (route: Route) => void;
  /** Optional override; defaults to the six Datawrap product routes. */
  surfaces?: readonly { label: string; route: Route; sub?: string }[];
}

const DEFAULT_SURFACES = [
  { label: "Transfer Studio", route: "product-transfer", sub: "Map → gates → write → prove" },
  { label: "Job Theater", route: "product-jobs", sub: "Live phases · quarantine · proof" },
  { label: "Pipelines", route: "product-pipelines", sub: "Schedules on the governed engine" },
  { label: "Query", route: "product-query", sub: "Safe preview · Studio handoff" },
  { label: "Pilot", route: "product-pilot", sub: "NL triage on real evidence" },
  { label: "MCP", route: "product-mcp", sub: "Agent tools · never raw passwords" },
] as const;

export function ProductSurfaceStrip<Route extends string>({
  onNavigate,
  surfaces,
}: ProductSurfaceStripProps<Route>) {
  const items = useMemo(
    () => (surfaces ?? (DEFAULT_SURFACES as unknown as readonly {
      label: string;
      route: Route;
      sub?: string;
    }[])),
    [surfaces],
  );

  return (
    <nav className="lp-cinema-strip" aria-label="Product surfaces">
      {items.map((s, i) => (
        <button
          key={s.label}
          type="button"
          className="lp-cinema-strip-item"
          style={{ "--i": i } as CSSProperties}
          onClick={() => onNavigate(s.route)}
        >
          <span className="lp-cinema-strip-label">{s.label}</span>
          {s.sub ? <span className="lp-cinema-strip-sub">{s.sub}</span> : null}
          <span className="lp-cinema-strip-arrow" aria-hidden>→</span>
        </button>
      ))}
    </nav>
  );
}

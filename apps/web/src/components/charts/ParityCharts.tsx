/**
 * Parity Studio charts: small SVG components in the house style (no chart
 * library). Every colour comes from a --df-* token through a class in
 * parity.css, so light and dark themes both hold. Each chart states its finding
 * in an aria-label, and every mark carries a <title> for hover.
 *
 * Scale maths lives in lib/chartScale.ts, where it is unit-tested.
 */

import { useId } from "react";
import { ProgressRing } from "../ui/ProgressRing";
import { useElementWidth } from "./useElementWidth";
import {
  linearScale,
  linePath,
  ordinalPositions,
  parityAxis,
  percentFraction,
  splitWidths,
} from "../../lib/chartScale";
import {
  HEAT_CELL_LABELS,
  checkLabel,
  formatParityPct,
  heatCellLabel,
  tableBarTone,
  tableParityLabel,
  tableStatusPresentation,
  badgeClass,
  type ParityHeatCell,
  type ParitySummary,
  type ParitySummaryRun,
  type ParityTable,
} from "../../lib/parity";

/* ── Parity ring ─────────────────────────────────────────────── */

const CONFIDENCE_WORDS = "95% confidence";

export interface ParityRingProps {
  /** 0..100; null when the evidence cannot say: the ring shows a dash and the caption (default "not compared"). */
  pct: number | null;
  tone?: "ok" | "warn" | "danger" | "idle";
  size?: number;
  caption?: string;
  /** The figure is a sampled lower bound: it reads "≥" and the caption says so. */
  lowerBound?: boolean;
  /** A sample that bounds nothing: the ring shows rows checked of the total, never a percentage. */
  checked?: { checked: number; total: number | null } | null;
}

/** The headline figure: share of rows proven equal, drawn unrounded. */
export function ParityRing({ pct, tone, size = 150, caption, lowerBound = false, checked = null }: ParityRingProps) {
  if (checked) {
    const share = checked.total ? Math.min(100, (100 * checked.checked) / checked.total) : 0;
    const of = checked.total != null ? ` of ${checked.total.toLocaleString("en-US")}` : "";
    return (
      <div
        className="df2-parity-ring"
        role="img"
        aria-label={`${checked.checked.toLocaleString("en-US")}${of} rows checked; the other rows were not compared`}
      >
        <ProgressRing value={share} exact size={size} radius={size / 2 - 10} tone="idle">
          <strong className="df2-parity-ring-value">{checked.checked.toLocaleString("en-US")}</strong>
          <small className="df2-parity-ring-caption">{caption ?? `rows checked${of}`}</small>
        </ProgressRing>
      </div>
    );
  }
  if (pct == null || !Number.isFinite(pct)) {
    // No figure: a compact dash (a word overflows the ring) and why, never a sampled bound.
    const why = caption ?? "not compared";
    return (
      <div className="df2-parity-ring" role="img" aria-label={`Share of rows proven equal is unknown (${why})`}>
        <ProgressRing value={0} exact size={size} radius={size / 2 - 10} tone="idle">
          <strong className="df2-parity-ring-value">—</strong>
          <small className="df2-parity-ring-caption">{why}</small>
        </ProgressRing>
      </div>
    );
  }
  const exact = formatParityPct(pct);
  const bounded = lowerBound && pct < 100;
  const label = bounded ? `≥ ${exact}` : exact;
  const resolvedCaption = caption ?? (lowerBound ? "rows equal (sampled bound)" : "rows proven equal");
  const resolvedTone = tone ?? "ok";
  const aria = lowerBound
    ? `At least ${exact} of rows equal, a ${CONFIDENCE_WORDS} bound from a sample`
    : `${exact} of rows proven equal`;
  return (
    <div className="df2-parity-ring" role="img" aria-label={aria}>
      <ProgressRing value={pct} exact size={size} radius={size / 2 - 10} tone={resolvedTone}>
        <strong className="df2-parity-ring-value">{label}</strong>
        <small className="df2-parity-ring-caption">{resolvedCaption}</small>
      </ProgressRing>
    </div>
  );
}

/* ── Per-table bars ──────────────────────────────────────────── */

/** One horizontal bar, 0..100, to scale. The figure beside it carries the meaning. */
export function ParityBar({ pct, tone = "brand" }: { pct: number | null; tone?: "brand" | "warn" | "muted" }) {
  const fraction = percentFraction(pct);
  return (
    <svg className={`df2-parity-bar is-${tone}`} width="100%" height="8" aria-hidden focusable="false">
      <rect className="df2-parity-bar-track" x="0" y="0" width="100%" height="8" rx="4" />
      {fraction > 0 && (
        <rect className="df2-parity-bar-fill" x="0" y="0" width={`${(fraction * 100).toFixed(3)}%`} height="8" rx="4" />
      )}
    </svg>
  );
}

/** The Tables card: name, proven-equal bar, how it was checked, and status. */
export function TableParityBars({ tables }: { tables: ParityTable[] }) {
  return (
    <div className="df2-parity-scroll">
      <table className="df2-table df2-parity-tables">
        <caption className="df2-sr-only">Parity per table</caption>
        <thead>
          <tr>
            <th scope="col">Table</th>
            <th scope="col" className="df2-parity-col-bar">Proven equal</th>
            <th scope="col">Check</th>
            <th scope="col">Status</th>
          </tr>
        </thead>
        <tbody>
          {tables.map((table) => {
            const status = tableStatusPresentation(table);
            return (
              <tr key={table.name}>
                <td className="df2-mono">{table.name}</td>
                <td>
                  <div className="df2-parity-bar-cell">
                    <ParityBar pct={table.proven_equal_pct} tone={tableBarTone(table)} />
                    <span className="df2-parity-num">{tableParityLabel(table)}</span>
                  </div>
                </td>
                <td>{checkLabel(table.check)}</td>
                <td>
                  <span className={badgeClass(status.tone)}>{status.label}</span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/* ── Explained vs unexplained ────────────────────────────────── */

export interface DifferenceSplitBarProps {
  explained: number;
  unexplained: number;
}

/** What is left before sign-off: explained rows on the left, unexplained on the right. */
export function DifferenceSplitBar({ explained, unexplained }: DifferenceSplitBarProps) {
  const [ref, width] = useElementWidth<HTMLDivElement>(320);
  const e = Math.max(0, explained);
  const u = Math.max(0, unexplained);
  const summary = `${e.toLocaleString("en-US")} explained, ${u.toLocaleString("en-US")} unexplained`;
  const w = splitWidths(e, u, width);
  return (
    <div ref={ref} className="df2-parity-split">
      <svg width={width} height={40} viewBox={`0 0 ${width} 40`} role="img" aria-label={summary}>
        <title>{summary}</title>
        {e + u === 0 ? (
          <rect className="df2-parity-split-empty" x="0" y="8" width={width} height="16" rx="4" />
        ) : (
          <>
            {w.first > 0 && <rect className="df2-parity-split-ok" x="0" y="8" width={w.first} height="16" rx="4" />}
            {w.second > 0 && (
              <rect className="df2-parity-split-bad" x={w.first + w.gap} y="8" width={w.second} height="16" rx="4" />
            )}
          </>
        )}
        <text className="df2-parity-chart-label" x="0" y="38">{`${e.toLocaleString("en-US")} explained`}</text>
        <text className="df2-parity-chart-label" x={width} y="38" textAnchor="end">
          {`${u.toLocaleString("en-US")} unexplained`}
        </text>
      </svg>
    </div>
  );
}

/* ── Parity trend ────────────────────────────────────────────── */

const TREND_PAD = { t: 18, r: 18, b: 30, l: 56 };

/** Near the top, the latest value sits under its point so it never lands on the line. */
function lastLabelY(pointY: number): number {
  return pointY < TREND_PAD.t + 24 ? pointY + 20 : pointY - 10;
}

function runLabel(run: Pick<ParitySummaryRun, "name" | "job_id">): string {
  const name = (run.name || "").trim() || run.job_id;
  return name.length > 22 ? `${name.slice(0, 21)}…` : name;
}

/** One sentence a screen reader hears instead of the line. */
/** A run compared only on a sample with no figure: drawn as "checked N of M", never as a value. */
export function isSampledOnly(run: Pick<ParitySummaryRun, "method" | "proven_equal_pct">): boolean {
  return run.proven_equal_pct == null && (run.method === "sampled" || run.method === "mixed");
}

function sampledTip(run: ParitySummaryRun): string {
  const of = run.rows_total != null ? ` of ${run.rows_total.toLocaleString("en-US")}` : "";
  const checked = run.rows_checked != null ? `${run.rows_checked.toLocaleString("en-US")}${of} rows checked` : "sampled";
  return `${run.name || run.job_id}: ${checked}; not compared in full, so no figure`;
}

export function trendSummary(runs: ParitySummaryRun[]): string {
  const known = runs.filter((r) => r.proven_equal_pct != null && Number.isFinite(r.proven_equal_pct));
  const sampled = runs.filter(isSampledOnly).length;
  const sampledText = sampled ? ` ${sampled} of ${runs.length} runs were only sampled and have no figure.` : "";
  if (known.length === 0) return `No parity figures for the last ${runs.length} runs yet.${sampledText}`;
  const values = known.map((r) => r.proven_equal_pct as number);
  const latest = known[known.length - 1];
  const flagged = runs.filter((r) => r.unexplained > 0).length;
  const range =
    Math.min(...values) === Math.max(...values)
      ? `${formatParityPct(values[0])} in every run with a figure`
      : `between ${formatParityPct(Math.min(...values))} and ${formatParityPct(Math.max(...values))}`;
  const flaggedText =
    flagged === 0 ? "No run had unexplained differences." : `${flagged} of ${runs.length} runs had unexplained differences.`;
  const drifted = runs.filter((r) => r.drift === true).length;
  const driftText = drifted ? ` ${drifted} of ${runs.length} runs drifted from their route's earlier runs.` : "";
  return `Parity over the last ${runs.length} runs: ${range}; latest ${formatParityPct(latest.proven_equal_pct)}. ${flaggedText}${driftText}${sampledText}`;
}

/**
 * Rows proven equal per run, oldest to newest: a line and its points on a
 * round axis that ends at 100% (lib/chartScale parityAxis), three labelled
 * gridlines, no filled band. Every point carries its run in a tooltip.
 */
export function ParityTrendChart({ runs, height = 180 }: { runs: ParitySummaryRun[]; height?: number }) {
  const [ref, width] = useElementWidth<HTMLDivElement>(360);
  const values = runs.map((r) => r.proven_equal_pct);
  const axis = parityAxis(values);
  const domain = axis?.domain ?? null;
  const summary = trendSummary(runs);

  if (!domain || !axis) {
    return (
      <div ref={ref} className="df2-parity-trend is-empty" role="img" aria-label={summary}>
        <p className="df2-parity-muted">{summary}</p>
      </div>
    );
  }

  const innerBottom = height - TREND_PAD.b;
  const y = linearScale(domain, [innerBottom, TREND_PAD.t]);
  const xs = ordinalPositions(runs.length, [TREND_PAD.l, Math.max(TREND_PAD.l + 1, width - TREND_PAD.r)]);
  const points = runs.map((r, i) => ({ x: xs[i], y: r.proven_equal_pct == null ? null : y(r.proven_equal_pct) }));
  const ticks = axis.ticks;
  let lastIndex = -1;
  for (let i = runs.length - 1; i >= 0; i -= 1) {
    if (points[i].y != null) {
      lastIndex = i;
      break;
    }
  }

  return (
    <div ref={ref} className="df2-parity-trend">
      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={summary}>
        <title>{summary}</title>
        {ticks.map((t) => t.value).map((v, i) => (
          <line
            key={`grid-${i}`}
            className="df2-parity-grid"
            x1={TREND_PAD.l}
            x2={width - TREND_PAD.r}
            y1={y(v)}
            y2={y(v)}
          />
        ))}
        {ticks.map((t) => (
          <text key={`tick-${t.kind}`} className="df2-parity-chart-label" x={TREND_PAD.l - 8} y={y(t.value) + 4} textAnchor="end">
            {t.label}
          </text>
        ))}
        <path className="df2-parity-trend-line" d={linePath(points)} />
        {runs.map((run, i) =>
          isSampledOnly(run) ? (
            // Sampled only: a hollow mark on the baseline, never a value on the scale.
            <circle
              key={`sampled-${run.job_id}-${i}`}
              className="df2-parity-point is-sampled"
              cx={points[i].x}
              cy={innerBottom}
              r={3.5}
            >
              <title>{sampledTip(run)}</title>
            </circle>
          ) : null,
        )}
        {runs.map((run, i) => {
          const py = points[i].y;
          if (py == null) return null;
          const isLast = i === lastIndex;
          const flagged = run.unexplained > 0;
          const drift = run.drift === true;
          const tip = `${run.name || run.job_id}: ${formatParityPct(run.proven_equal_pct)}${
            flagged ? ` · ${run.unexplained.toLocaleString("en-US")} unexplained` : ""
          }${drift ? " · drift: an outlier against this route's earlier runs" : ""}`;
          return (
            <g key={`${run.job_id}-${i}`}>
              {/* A wider invisible target so the tooltip is reachable. */}
              <circle className="df2-parity-hit" cx={points[i].x} cy={py} r={9}>
                <title>{tip}</title>
              </circle>
              <circle
                className={`df2-parity-point${flagged ? " is-flagged" : ""}${isLast ? " is-last" : ""}`}
                cx={points[i].x}
                cy={py}
                r={isLast ? 5 : flagged ? 4.5 : 3}
              >
                <title>{tip}</title>
              </circle>
              {/* Robust z against the route's earlier runs (API); a word, not only a colour. */}
              {drift && (
                <text className="df2-parity-drift" x={points[i].x} y={py > TREND_PAD.t + 14 ? py - 9 : py + 16} textAnchor="middle">
                  drift
                </text>
              )}
            </g>
          );
        })}
        {lastIndex >= 0 && points[lastIndex].y != null && (
          <text
            className="df2-parity-chart-value"
            x={runs.length > 1 ? points[lastIndex].x - 8 : points[lastIndex].x}
            y={lastLabelY(points[lastIndex].y as number)}
            textAnchor={runs.length > 1 ? "end" : "middle"}
          >
            {formatParityPct(runs[lastIndex].proven_equal_pct)}
          </text>
        )}
        {runs.length > 0 && (
          <text className="df2-parity-chart-label" x={xs[0]} y={height - 8} textAnchor={runs.length > 1 ? "start" : "middle"}>
            {runLabel(runs[0])}
          </text>
        )}
        {runs.length > 1 && (
          <text className="df2-parity-chart-label" x={xs[xs.length - 1]} y={height - 8} textAnchor="end">
            {runLabel(runs[runs.length - 1])}
          </text>
        )}
      </svg>
    </div>
  );
}

/* ── Tables × runs heatmap ───────────────────────────────────── */

const HEAT_ORDER: ParityHeatCell[] = [
  "proven",
  "sampled",
  "explained",
  "unexplained_small",
  "unexplained_large",
  "failed",
  "none",
];
const HEAT_KNOWN = new Set<string>(HEAT_ORDER);

function heatClass(cell: string): string {
  return HEAT_KNOWN.has(cell) ? cell.replace("_", "-") : "none";
}

/**
 * Every state carries its own shape, so no two states differ by colour alone:
 * proven a check, sampled a ring, explained a dash, unexplained a dot (larger
 * when large), failed verification a cross, not checked a slash.
 */
function HeatGlyph({ cell, cx, cy }: { cell: string; cx: number; cy: number }) {
  const state = HEAT_KNOWN.has(cell) ? cell : "none";
  const cls = `df2-parity-heat-glyph is-${heatClass(state)}`;
  switch (state) {
    case "proven":
      return <polyline className={cls} points={`${cx - 3.5},${cy} ${cx - 1},${cy + 2.5} ${cx + 3.5},${cy - 2.5}`} aria-hidden />;
    case "sampled":
      return <circle className={cls} cx={cx} cy={cy} r={2.8} aria-hidden />;
    case "explained":
      return <line className={cls} x1={cx - 3.5} y1={cy} x2={cx + 3.5} y2={cy} aria-hidden />;
    case "unexplained_small":
    case "unexplained_large":
      return <circle className="df2-parity-heat-mark" cx={cx} cy={cy} r={state === "unexplained_large" ? 3.5 : 2} aria-hidden />;
    case "failed":
      return (
        <path
          className={cls}
          d={`M${cx - 3} ${cy - 3} L${cx + 3} ${cy + 3} M${cx + 3} ${cy - 3} L${cx - 3} ${cy + 3}`}
          aria-hidden
        />
      );
    default:
      // Not run / not checked: an empty cell. A slash in every gap was noise;
      // the blank (and the legend) say it, and every checked state has a mark.
      return null;
  }
}

/** Which table keeps drifting, in words. */
export function heatmapSummary(heatmap: ParitySummary["heatmap"]): string {
  const runs = heatmap.job_ids.length;
  const tables = heatmap.tables.length;
  if (!tables || !runs) return "No tables checked yet.";
  const drifting = heatmap.tables
    .map((name, i) => ({
      name,
      count: (heatmap.cells[i] ?? []).filter((c) => c === "unexplained_small" || c === "unexplained_large").length,
    }))
    .filter((t) => t.count > 0)
    .sort((a, b) => b.count - a.count);
  const failed = heatmap.tables
    .map((name, i) => ({ name, count: (heatmap.cells[i] ?? []).filter((c) => c === "failed").length }))
    .filter((t) => t.count > 0)
    .sort((a, b) => b.count - a.count);
  const head = `${tables} ${tables === 1 ? "table" : "tables"} over ${runs} ${runs === 1 ? "run" : "runs"}.`;
  const failedPart = failed.length ? ` Failed verification: ${tableCounts(failed)}.` : "";
  if (!drifting.length) return `${head} No table had unexplained differences.${failedPart}`;
  return `${head} Unexplained differences: ${tableCounts(drifting)}.${failedPart}`;
}

/** "payments in 2 runs; orders in 1 run" for the first three tables, then how many more. */
function tableCounts(items: { name: string; count: number }[]): string {
  const parts = items
    .slice(0, 3)
    .map((t) => `${t.name} in ${t.count} ${t.count === 1 ? "run" : "runs"}`)
    .join("; ");
  return parts + (items.length > 3 ? `; ${items.length - 3} more` : "");
}

export interface TableRunHeatmapProps {
  heatmap: ParitySummary["heatmap"];
  /** Run names for tooltips; matched by job id. */
  runs?: ParitySummaryRun[];
}

const HEAT_LABEL_W = 116;
const HEAT_GAP = 3;
const HEAT_ROW_H = 20;

/** Tables down the side, runs across (oldest to newest), one cell per check. */
export function TableRunHeatmap({ heatmap, runs = [] }: TableRunHeatmapProps) {
  const [ref, width] = useElementWidth<HTMLDivElement>(360);
  const cols = heatmap.job_ids.length;
  const rows = heatmap.tables.length;
  const summary = heatmapSummary(heatmap);
  const names = new Map(runs.map((r) => [r.job_id, r.name || r.job_id]));
  // Cells are 14–28px wide: square-ish marks, not stretched bars; below 14px the card scrolls sideways.
  const cellW = cols ? Math.min(28, Math.max(14, (width - HEAT_LABEL_W - HEAT_GAP * cols) / cols)) : 14;
  const svgW = HEAT_LABEL_W + cols * (cellW + HEAT_GAP);
  const svgH = rows * (HEAT_ROW_H + HEAT_GAP);
  const tableId = useId();

  return (
    <div className="df2-parity-heat">
      <div ref={ref} className="df2-parity-scroll">
        <svg
          width={svgW}
          height={svgH}
          viewBox={`0 0 ${svgW} ${svgH}`}
          role="img"
          aria-label={summary}
          aria-describedby={tableId}
        >
          <title>{summary}</title>
          {heatmap.tables.map((table, r) => {
            const top = r * (HEAT_ROW_H + HEAT_GAP);
            const label = table.length > 16 ? `${table.slice(0, 15)}…` : table;
            return (
              <g key={table}>
                <text className="df2-parity-heat-label" x={0} y={top + HEAT_ROW_H / 2 + 4}>
                  <title>{table}</title>
                  {label}
                </text>
                {heatmap.job_ids.map((jobId, c) => {
                  const cell = heatmap.cells[r]?.[c] ?? "none";
                  const x = HEAT_LABEL_W + c * (cellW + HEAT_GAP);
                  const tip = `${table} · ${names.get(jobId) ?? jobId}: ${heatCellLabel(cell)}`;
                  return (
                    <g key={`${jobId}-${c}`}>
                      <rect className={`df2-parity-heat-cell is-${heatClass(cell)}`} x={x} y={top} width={cellW} height={HEAT_ROW_H} rx="3">
                        <title>{tip}</title>
                      </rect>
                      {/* A shape per state, so the state never rests on colour alone. */}
                      <HeatGlyph cell={cell} cx={x + cellW / 2} cy={top + HEAT_ROW_H / 2} />
                    </g>
                  );
                })}
              </g>
            );
          })}
        </svg>
      </div>
      {/* The cells as a table for screen readers: titles inside role="img" are not reachable. */}
      <table id={tableId} className="df2-sr-only">
        <caption>Parity per table and run</caption>
        <thead>
          <tr>
            <th scope="col">Table</th>
            {heatmap.job_ids.map((jobId, c) => (
              <th scope="col" key={`${jobId}-${c}`}>{names.get(jobId) ?? jobId}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {heatmap.tables.map((table, r) => (
            <tr key={table}>
              <th scope="row">{table}</th>
              {heatmap.job_ids.map((jobId, c) => (
                <td key={`${jobId}-${c}`}>{heatCellLabel(heatmap.cells[r]?.[c] ?? "none")}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      <ul className="df2-parity-legend" aria-label="Legend">
        {HEAT_ORDER.map((cell) => (
          <li key={cell}>
            <svg className={`df2-parity-legend-swatch is-${heatClass(cell)}`} width="12" height="12" viewBox="0 0 12 12" aria-hidden>
              {/* Fill inherits the swatch's state colour. */}
              <rect x="0" y="0" width="12" height="12" rx="2" />
              <HeatGlyph cell={cell} cx={6} cy={6} />
            </svg>
            {HEAT_CELL_LABELS[cell]}
          </li>
        ))}
      </ul>
    </div>
  );
}

/**
 * Parity Studio charts: small SVG components in the house style (no chart
 * library). Every colour comes from a --df-* token through a class in
 * parity.css, so light and dark themes both hold. Each chart states its finding
 * in an aria-label, and every mark carries a <title> for hover.
 *
 * Scale maths lives in lib/chartScale.ts, where it is unit-tested.
 */

import { ProgressRing } from "../ui/ProgressRing";
import { useElementWidth } from "./useElementWidth";
import {
  areaPath,
  linearScale,
  linePath,
  minMaxTicks,
  ordinalPositions,
  percentDomain,
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

export interface ParityRingProps {
  /** 0..100; null when the evidence cannot say. */
  pct: number | null;
  tone?: "ok" | "warn" | "danger" | "idle";
  size?: number;
  caption?: string;
}

/** The headline figure: share of rows proven equal, drawn unrounded. */
export function ParityRing({ pct, tone, size = 150, caption = "rows proven equal" }: ParityRingProps) {
  const label = formatParityPct(pct);
  const resolvedTone = pct == null ? "idle" : tone ?? "ok";
  return (
    <div
      className="df2-parity-ring"
      role="img"
      aria-label={pct == null ? "Share of rows proven equal is unknown" : `${label} of rows proven equal`}
    >
      <ProgressRing value={pct ?? 0} exact size={size} radius={size / 2 - 10} tone={resolvedTone}>
        <strong className="df2-parity-ring-value">{label}</strong>
        <small className="df2-parity-ring-caption">{caption}</small>
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
export function trendSummary(runs: ParitySummaryRun[]): string {
  const known = runs.filter((r) => r.proven_equal_pct != null && Number.isFinite(r.proven_equal_pct));
  if (known.length === 0) return `No parity figures for the last ${runs.length} runs yet.`;
  const values = known.map((r) => r.proven_equal_pct as number);
  const latest = known[known.length - 1];
  const flagged = runs.filter((r) => r.unexplained > 0).length;
  const range =
    Math.min(...values) === Math.max(...values)
      ? `${formatParityPct(values[0])} in every run with a figure`
      : `between ${formatParityPct(Math.min(...values))} and ${formatParityPct(Math.max(...values))}`;
  const flaggedText =
    flagged === 0 ? "No run had unexplained differences." : `${flagged} of ${runs.length} runs had unexplained differences.`;
  return `Parity over the last ${runs.length} runs: ${range}; latest ${formatParityPct(latest.proven_equal_pct)}. ${flaggedText}`;
}

/** Line + area of rows proven equal per run, oldest to newest, on a labelled min/max scale. */
export function ParityTrendChart({ runs, height = 180 }: { runs: ParitySummaryRun[]; height?: number }) {
  const [ref, width] = useElementWidth<HTMLDivElement>(360);
  const values = runs.map((r) => r.proven_equal_pct);
  const domain = percentDomain(values);
  const summary = trendSummary(runs);

  if (!domain) {
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
  const ticks = minMaxTicks(domain, values);
  const mid = (domain[0] + domain[1]) / 2;
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
        {[...ticks.map((t) => t.value), mid].map((v, i) => (
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
        <path className="df2-parity-trend-area" d={areaPath(points, innerBottom)} />
        <path className="df2-parity-trend-line" d={linePath(points)} />
        {runs.map((run, i) => {
          const py = points[i].y;
          if (py == null) return null;
          const isLast = i === lastIndex;
          const flagged = run.unexplained > 0;
          const tip = `${run.name || run.job_id}: ${formatParityPct(run.proven_equal_pct)}${
            flagged ? ` · ${run.unexplained.toLocaleString("en-US")} unexplained` : ""
          }`;
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

const HEAT_ORDER: ParityHeatCell[] = ["proven", "explained", "unexplained_small", "unexplained_large", "none"];
const HEAT_KNOWN = new Set<string>(HEAT_ORDER);

function heatClass(cell: string): string {
  return HEAT_KNOWN.has(cell) ? cell.replace("_", "-") : "none";
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
  const head = `${tables} ${tables === 1 ? "table" : "tables"} over ${runs} ${runs === 1 ? "run" : "runs"}.`;
  if (!drifting.length) return `${head} No table had unexplained differences.`;
  const parts = drifting
    .slice(0, 3)
    .map((t) => `${t.name} in ${t.count} ${t.count === 1 ? "run" : "runs"}`)
    .join("; ");
  const more = drifting.length > 3 ? `; ${drifting.length - 3} more` : "";
  return `${head} Unexplained differences: ${parts}${more}.`;
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
  // Cells stay at least 14px wide; below that the card scrolls sideways.
  const cellW = cols ? Math.max(14, (width - HEAT_LABEL_W - HEAT_GAP * cols) / cols) : 14;
  const svgW = HEAT_LABEL_W + cols * (cellW + HEAT_GAP);
  const svgH = rows * (HEAT_ROW_H + HEAT_GAP);

  return (
    <div className="df2-parity-heat">
      <div ref={ref} className="df2-parity-scroll">
        <svg width={svgW} height={svgH} viewBox={`0 0 ${svgW} ${svgH}`} role="img" aria-label={summary}>
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
                  const unexplained = cell === "unexplained_small" || cell === "unexplained_large";
                  return (
                    <g key={`${jobId}-${c}`}>
                      <rect className={`df2-parity-heat-cell is-${heatClass(cell)}`} x={x} y={top} width={cellW} height={HEAT_ROW_H} rx="3">
                        <title>{tip}</title>
                      </rect>
                      {/* A mark inside unexplained cells, so the state never rests on colour alone. */}
                      {unexplained && (
                        <circle
                          className="df2-parity-heat-mark"
                          cx={x + cellW / 2}
                          cy={top + HEAT_ROW_H / 2}
                          r={cell === "unexplained_large" ? 3.5 : 2}
                          aria-hidden
                        />
                      )}
                    </g>
                  );
                })}
              </g>
            );
          })}
        </svg>
      </div>
      <ul className="df2-parity-legend" aria-label="Legend">
        {HEAT_ORDER.map((cell) => (
          <li key={cell}>
            <span className={`df2-parity-legend-swatch is-${heatClass(cell)}`} aria-hidden />
            {HEAT_CELL_LABELS[cell]}
          </li>
        ))}
      </ul>
    </div>
  );
}

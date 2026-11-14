/**
 * Reversibility Proof charts, in the Parity Studio house style: small SVGs,
 * every colour a --df-* token through a class (reversibility.css), every chart
 * states its finding in an aria-label, and the scale maths is lib/chartScale.ts.
 *
 * Durations are drawn only where they were measured. A projection is a hollow,
 * dashed mark labelled "projection"; it is never drawn as a measurement.
 */

import { useElementWidth } from "./useElementWidth";
import { linearScale, linePath, ordinalPositions, valueDomain } from "../../lib/chartScale";
import {
  formatDuration,
  formatWindow,
  rehearsalVerdictPresentation,
  windowRatio,
  type Trend,
} from "../../lib/exitReadiness";

/* ── Measured vs window ──────────────────────────────────────── */

export interface WindowBarProps {
  measured: number | null | undefined;
  window: number | null | undefined;
  /**
   * The API's verdict on the fit; drawing never decides it. Null with a
   * measurement means "not compared" (only a proven run is, contract 10.1.4-5):
   * the bar is drawn to scale but never marked over the window.
   */
  fits?: boolean | null;
  size?: "sm" | "lg";
}

/**
 * The window is the whole track; the fill is the measured exit, to scale. Over
 * the window the bar fills and turns to the danger token, and the words say
 * "does not fit" (never colour alone). Nothing measured draws no fill.
 */
export function WindowBar({ measured, window, fits, size = "sm" }: WindowBarProps) {
  const ratio = windowRatio(measured, window);
  const notCompared = fits === null && ratio != null;
  const over = !notCompared && (fits === false || Boolean(ratio?.over));
  const label =
    ratio == null
      ? `Not measured against a ${formatWindow(window)} window`
      : `${formatDuration(measured)} measured of a ${formatWindow(window)} window, ${ratio.label}${
          notCompared ? ", not compared with the window (only proven runs are)" : over ? ", does not fit" : ""
        }`;
  const height = size === "lg" ? 12 : 6;
  // A fit under 1% still shows a sliver, so a fast exit does not read as "nothing ran".
  const pct = ratio == null ? 0 : Math.max(ratio.fraction * 100, ratio.ratio > 0 ? 1.5 : 0);
  return (
    <svg
      className={`df2-exit-window-bar is-${size}${over ? " is-over" : ""}${ratio == null ? " is-unmeasured" : ""}${notCompared ? " is-uncompared" : ""}`}
      width="100%"
      height={height}
      role="img"
      aria-label={label}
      focusable="false"
    >
      <title>{label}</title>
      <rect className="df2-exit-window-track" x="0" y="0" width="100%" height={height} rx={height / 2} />
      {pct > 0 && (
        <rect className="df2-exit-window-fill" x="0" y="0" width={`${pct.toFixed(2)}%`} height={height} rx={height / 2} />
      )}
    </svg>
  );
}

/* ── Duration trend ──────────────────────────────────────────── */

const PAD = { t: 16, r: 20, b: 28, l: 56 };

/** Rehearsals without a measured duration: they get no value on the chart, only a mention. */
export function unmeasuredRuns(trend: Trend): string[] {
  return (trend.points ?? [])
    .filter((p) => p.measured_seconds == null)
    .map((p) => `rehearsal ${p.n} (${rehearsalVerdictPresentation(p.verdict).label.toLowerCase()})`);
}

/** One sentence a screen reader hears instead of the chart. */
export function durationTrendSummary(trend: Trend, windowSeconds: number): string {
  const points = trend.points ?? [];
  const measured = points.filter((p) => p.measured_seconds != null);
  if (!points.length) return "No measured rehearsals to trend yet.";
  if (!measured.length) return `${points.length} rehearsal${points.length === 1 ? "" : "s"}, none with a measured duration yet.`;
  const latest = measured[measured.length - 1];
  const longest = Math.max(...measured.map((p) => p.measured_seconds as number));
  const missing = unmeasuredRuns(trend);
  const missingText = missing.length ? ` Not measured, so not drawn: ${missing.join(", ")}.` : "";
  const projection = trend.projection ? ` A projection (not measured) puts the next exit at about ${formatDuration(trend.projection.seconds)}.` : "";
  return `Measured exit over ${measured.length} rehearsal${measured.length === 1 ? "" : "s"}: latest ${formatDuration(latest.measured_seconds)}, longest ${formatDuration(longest)}, against a ${formatWindow(windowSeconds)} window.${missingText}${projection}`;
}

/**
 * Measured duration per rehearsal, oldest to newest, on a labelled min/max
 * scale (lib/chartScale valueDomain): the marks use the plot instead of a
 * zero baseline. A rehearsal with no measured duration has no value, so it gets
 * no point: the line passes it by with a dotted gap and the caption names it.
 * The window line is drawn when it sits on the same scale; when it is far above
 * the measurements, the chart says so instead of squashing every point.
 */
export function DurationTrendChart({ trend, windowSeconds, height = 140 }: { trend: Trend; windowSeconds: number; height?: number }) {
  const [ref, width] = useElementWidth<HTMLDivElement>(360);
  const points = trend.points ?? [];
  const summary = durationTrendSummary(trend, windowSeconds);
  const measuredValues = points.map((p) => p.measured_seconds).filter((v): v is number => v != null && Number.isFinite(v));
  const projection = trend.projection && trend.projection.kind === "projection" ? trend.projection : null;
  const missing = unmeasuredRuns(trend);
  if (!measuredValues.length) {
    return (
      <div ref={ref} className="df2-exit-trend is-empty" role="img" aria-label={summary}>
        <p className="df2-parity-muted">{summary}</p>
      </div>
    );
  }
  const dataMax = Math.max(...measuredValues, projection?.seconds ?? 0);
  const dataMin = Math.min(...measuredValues, projection?.seconds ?? Infinity);
  // The window shares the scale only when it is within 3x of the data; otherwise it is stated in words.
  const windowOnScale = windowSeconds > 0 && windowSeconds <= dataMax * 3;
  const domain = valueDomain([dataMin, dataMax, windowOnScale ? windowSeconds : null]) ?? [0, 1];
  const innerBottom = height - PAD.b;
  const y = linearScale(domain, [innerBottom, PAD.t]);
  const slots = points.length + (projection ? 1 : 0);
  const xs = ordinalPositions(slots, [PAD.l, Math.max(PAD.l + 1, width - PAD.r)]);
  const plotted = points.map((p, i) => ({ x: xs[i], y: p.measured_seconds == null ? null : y(p.measured_seconds) }));
  const known = plotted.map((pt, i) => ({ ...pt, i })).filter((pt): pt is { x: number; y: number; i: number } => pt.y != null);
  const last = known.length ? known[known.length - 1].i : -1;
  // Dotted bridges across rehearsals that were not measured: a gap in the data, not a value.
  const bridges = known.slice(1).map((pt, k) => [known[k], pt] as const).filter(([a, b]) => b.i - a.i > 1);
  const projX = projection ? xs[xs.length - 1] : 0;
  const projY = projection ? y(projection.seconds) : 0;
  const ticks = dataMax === dataMin ? [dataMax] : [dataMax, dataMin];

  return (
    <div ref={ref} className="df2-exit-trend">
      <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={summary}>
        <title>{summary}</title>
        {ticks.map((v, i) => (
          <g key={`tick-${i}`}>
            <line className="df2-parity-grid" x1={PAD.l} x2={width - PAD.r} y1={y(v)} y2={y(v)} />
            <text className="df2-parity-chart-label" x={PAD.l - 8} y={y(v) + 4} textAnchor="end">
              {formatDuration(v)}
            </text>
          </g>
        ))}
        {windowOnScale && (
          <g>
            <line className="df2-exit-trend-window" x1={PAD.l} x2={width - PAD.r} y1={y(windowSeconds)} y2={y(windowSeconds)} />
            <text className="df2-parity-chart-label" x={width - PAD.r} y={y(windowSeconds) - 6} textAnchor="end">
              {`window ${formatWindow(windowSeconds)}`}
            </text>
          </g>
        )}
        <path className="df2-parity-trend-line" d={linePath(plotted)} />
        {bridges.map(([a, b]) => (
          <line key={`gap-${a.i}-${b.i}`} className="df2-exit-trend-gap" x1={a.x} y1={a.y} x2={b.x} y2={b.y} />
        ))}
        {projection && last >= 0 && (
          <line className="df2-exit-trend-projection" x1={plotted[last].x} y1={plotted[last].y as number} x2={projX} y2={projY} />
        )}
        {points.map((p, i) => {
          const py = plotted[i].y;
          if (py == null) return null;
          const tip = `Rehearsal ${p.n}: ${formatDuration(p.measured_seconds)} measured (${rehearsalVerdictPresentation(p.verdict).label})`;
          return (
            <g key={p.rehearsal_id}>
              <circle className="df2-parity-hit" cx={plotted[i].x} cy={py} r={9}>
                <title>{tip}</title>
              </circle>
              <circle className={`df2-parity-point${i === last ? " is-last" : ""}`} cx={plotted[i].x} cy={py} r={i === last ? 5 : 3}>
                <title>{tip}</title>
              </circle>
            </g>
          );
        })}
        {projection && (
          <g>
            <circle className="df2-exit-trend-projection-point" cx={projX} cy={projY} r={4.5}>
              <title>{`Projection, not measured: about ${formatDuration(projection.seconds)}. ${projection.basis}`}</title>
            </circle>
            <text className="df2-parity-chart-label" x={projX} y={projY > PAD.t + 16 ? projY - 10 : projY + 24} textAnchor="end">
              projection
            </text>
          </g>
        )}
        {points.length > 0 && (
          <text className="df2-parity-chart-label" x={xs[0]} y={height - 8} textAnchor={slots > 1 ? "start" : "middle"}>
            {`#${points[0].n}`}
          </text>
        )}
        {points.length > 1 && (
          <text className="df2-parity-chart-label" x={xs[points.length - 1]} y={height - 8} textAnchor={projection ? "middle" : "end"}>
            {`#${points[points.length - 1].n}`}
          </text>
        )}
      </svg>
      {missing.length > 0 && (
        <p className="df2-parity-muted">
          Not measured, so not drawn: {missing.join(", ")}. The dotted line only bridges the gap; it is not a value.
        </p>
      )}
      {!windowOnScale && windowSeconds > 0 && (
        <p className="df2-parity-muted">
          The {formatWindow(windowSeconds)} window is far above this scale
          {dataMax > 0 ? ` (${Math.round(windowSeconds / dataMax).toLocaleString("en-US")}× the longest measurement)` : ""}, so it is not drawn.
        </p>
      )}
    </div>
  );
}

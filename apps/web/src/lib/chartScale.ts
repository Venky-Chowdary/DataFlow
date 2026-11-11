/**
 * Scale maths for the hand-drawn SVG charts.
 *
 * Pure functions, so "drawn to scale" is something a test can check: a value
 * maps to the same pixel every time, a gap in the data stays a gap in the line,
 * and a tiny segment is still visible without stealing width from its neighbour.
 */

export type Domain = readonly [number, number];
export type Range = readonly [number, number];

/** Linear map from a domain to a range. A zero-width domain maps to the range middle. */
export function linearScale(domain: Domain, range: Range): (value: number) => number {
  const [d0, d1] = domain;
  const [r0, r1] = range;
  const span = d1 - d0;
  if (!Number.isFinite(span) || span === 0) {
    const mid = (r0 + r1) / 2;
    return () => mid;
  }
  return (value: number) => r0 + ((value - d0) / span) * (r1 - r0);
}

export function finiteValues(values: ReadonlyArray<number | null | undefined>): number[] {
  return values.filter((v): v is number => typeof v === "number" && Number.isFinite(v));
}

/**
 * Y domain for a percentage series: from the lowest to the highest value seen,
 * never outside 0..100, and at least ``minSpan`` wide so a flat series at 100%
 * still has a scale under it instead of a divide-by-zero.
 */
export function percentDomain(
  values: ReadonlyArray<number | null | undefined>,
  minSpan = 0.5,
): Domain | null {
  const finite = finiteValues(values).map((v) => Math.max(0, Math.min(100, v)));
  if (finite.length === 0) return null;
  let lo = Math.min(...finite);
  let hi = Math.max(...finite);
  if (hi - lo < minSpan) {
    lo = hi - minSpan;
    if (lo < 0) {
      lo = 0;
      hi = Math.min(100, minSpan);
    }
  }
  return [lo, hi];
}

/** A percentage tick label with only the precision the axis needs. */
export function formatPercentTick(value: number): string {
  if (!Number.isFinite(value)) return "";
  if (value >= 100) return "100%";
  const decimals = Number.isInteger(value) ? 0 : Math.abs(value * 10 - Math.round(value * 10)) < 1e-9 ? 1 : 2;
  // Floor, like the report figures: a tick never claims more parity than the data.
  const factor = 10 ** decimals;
  return `${(Math.floor(value * factor + 1e-9) / factor).toFixed(decimals)}%`;
}

export interface AxisTick {
  value: number;
  label: string;
  kind: "min" | "max" | "floor";
}

/**
 * Labelled ticks for a trend: the series minimum and maximum, placed where those
 * values sit on the scale. A flat series has one value, so the domain floor is
 * labelled too and the reader can see how much room the line has.
 */
export function minMaxTicks(domain: Domain, values: ReadonlyArray<number | null | undefined>): AxisTick[] {
  const finite = finiteValues(values);
  if (finite.length === 0) return [];
  const min = Math.min(...finite);
  const max = Math.max(...finite);
  const ticks: AxisTick[] = [{ value: max, label: formatPercentTick(max), kind: "max" }];
  if (min !== max) ticks.push({ value: min, label: formatPercentTick(min), kind: "min" });
  else if (domain[0] < min) ticks.push({ value: domain[0], label: formatPercentTick(domain[0]), kind: "floor" });
  return ticks;
}

/** Evenly spaced x positions for ordinal points (runs); a single point sits in the middle. */
export function ordinalPositions(count: number, range: Range): number[] {
  if (count <= 0) return [];
  const [r0, r1] = range;
  if (count === 1) return [(r0 + r1) / 2];
  const step = (r1 - r0) / (count - 1);
  return Array.from({ length: count }, (_, i) => r0 + step * i);
}

export interface PlotPoint {
  x: number;
  /** Null where the value is unknown; the line breaks there rather than invent one. */
  y: number | null;
}

function fmt(n: number): string {
  return Number(n.toFixed(2)).toString();
}

/** Contiguous runs of known points. */
export function lineSegments(points: ReadonlyArray<PlotPoint>): Array<Array<{ x: number; y: number }>> {
  const segments: Array<Array<{ x: number; y: number }>> = [];
  let current: Array<{ x: number; y: number }> = [];
  for (const p of points) {
    if (p.y == null || !Number.isFinite(p.y)) {
      if (current.length) segments.push(current);
      current = [];
      continue;
    }
    current.push({ x: p.x, y: p.y });
  }
  if (current.length) segments.push(current);
  return segments;
}

/** SVG path for a line that breaks across unknown values. */
export function linePath(points: ReadonlyArray<PlotPoint>): string {
  return lineSegments(points)
    .map((seg) => seg.map((p, i) => `${i === 0 ? "M" : "L"}${fmt(p.x)},${fmt(p.y)}`).join(" "))
    .join(" ");
}

/** SVG path for the area under each known segment, closed down to ``baseline``. */
export function areaPath(points: ReadonlyArray<PlotPoint>, baseline: number): string {
  return lineSegments(points)
    .filter((seg) => seg.length > 1)
    .map((seg) => {
      const top = seg.map((p, i) => `${i === 0 ? "M" : "L"}${fmt(p.x)},${fmt(p.y)}`).join(" ");
      const last = seg[seg.length - 1];
      return `${top} L${fmt(last.x)},${fmt(baseline)} L${fmt(seg[0].x)},${fmt(baseline)} Z`;
    })
    .join(" ");
}

/**
 * Widths for a two-part bar (explained | unexplained) that fills ``width`` with
 * a ``gap`` between the parts. A non-zero part is never thinner than
 * ``minVisible`` px, so one unexplained row among a million still shows.
 */
export function splitWidths(
  first: number,
  second: number,
  width: number,
  gap = 2,
  minVisible = 4,
): { first: number; second: number; gap: number } {
  const a = Math.max(0, Number.isFinite(first) ? first : 0);
  const b = Math.max(0, Number.isFinite(second) ? second : 0);
  const total = a + b;
  if (total === 0 || width <= 0) return { first: 0, second: 0, gap: 0 };
  if (a === 0) return { first: 0, second: width, gap: 0 };
  if (b === 0) return { first: width, second: 0, gap: 0 };
  const usable = Math.max(0, width - gap);
  const floor = Math.min(minVisible, usable / 2);
  let wa = (a / total) * usable;
  wa = Math.min(Math.max(wa, floor), usable - floor);
  return { first: wa, second: usable - wa, gap };
}

/** Fill fraction (0..1) for a 0..100 percentage; unknown is empty, not full. */
export function percentFraction(pct: number | null | undefined): number {
  if (pct == null || !Number.isFinite(pct)) return 0;
  return Math.max(0, Math.min(100, pct)) / 100;
}

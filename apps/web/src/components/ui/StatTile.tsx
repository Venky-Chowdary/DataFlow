import type { ReactNode } from "react";

export type StatTone = "neutral" | "ok" | "warn" | "danger";

export interface StatTileProps {
  label: string;
  value: ReactNode;
  /** A delta, a ratio ("of 7 runs") or a short qualifier under the value. */
  caption?: ReactNode;
  /** A tiny chart beside the caption (decorative; the caption says it in words). */
  spark?: ReactNode;
  /** Status as a small dot beside the label, never a coloured block. */
  tone?: StatTone;
  mono?: boolean;
}

/**
 * The one stat tile: label, value, and a caption or sparkline, on the neutral
 * card surface. Status is a small dot, so a row of tiles reads calm. Renders a
 * dt/dd group: put tiles in a `StatTileRow` (a <dl>).
 * Same shape as the design system's StatCard (label, value, delta).
 */
export function StatTile({ label, value, caption, spark, tone = "neutral", mono = false }: StatTileProps) {
  return (
    <div className="df2-stat-tile" data-tone={tone}>
      <dt className="df2-stat-tile-label">
        {tone !== "neutral" ? <span className="df2-stat-tile-dot" aria-hidden="true" /> : null}
        {label}
      </dt>
      <dd className={`df2-stat-tile-value${mono ? " df2-mono" : ""}`}>{value}</dd>
      {caption || spark ? (
        <dd className="df2-stat-tile-foot">
          {spark}
          {caption ? <span className="df2-stat-tile-caption">{caption}</span> : null}
        </dd>
      ) : null}
    </div>
  );
}

export function StatTileRow({ children, label, className = "" }: { children: ReactNode; label: string; className?: string }) {
  return (
    <dl className={`df2-stat-tiles ${className}`.trim()} aria-label={label}>
      {children}
    </dl>
  );
}

/**
 * A 64×20 trend line for a stat tile. Values are drawn on ``domain`` (default
 * their own min..max); a null breaks the line. Decorative: aria-hidden.
 */
export function Sparkline({
  values,
  domain,
  kind = "line",
}: {
  values: Array<number | null | undefined>;
  domain?: readonly [number, number];
  kind?: "line" | "bars";
}) {
  const W = 64;
  const H = 20;
  const finite = values.filter((v): v is number => typeof v === "number" && Number.isFinite(v));
  if (values.length === 0 || finite.length === 0) return null;
  const lo = domain ? domain[0] : Math.min(...finite);
  const hi = domain ? domain[1] : Math.max(...finite);
  const span = hi - lo || 1;
  const y = (v: number) => H - 2 - ((Math.max(lo, Math.min(hi, v)) - lo) / span) * (H - 4);
  const step = values.length > 1 ? (W - 4) / (values.length - 1) : 0;
  const x = (i: number) => (values.length > 1 ? 2 + i * step : W / 2);
  if (kind === "bars") {
    const bw = Math.max(2, Math.min(6, (W - 4) / values.length - 2));
    return (
      <svg className="df2-spark is-bars" width={W} height={H} viewBox={`0 0 ${W} ${H}`} aria-hidden="true" focusable="false">
        <line className="df2-spark-base" x1="0" x2={W} y1={H - 1} y2={H - 1} />
        {values.map((v, i) =>
          typeof v === "number" && v > 0 ? (
            <rect key={i} className="df2-spark-bar" x={x(i) - bw / 2} y={y(v)} width={bw} height={H - 1 - y(v)} rx="1" />
          ) : null,
        )}
      </svg>
    );
  }
  let d = "";
  let pen = false;
  values.forEach((v, i) => {
    if (typeof v !== "number" || !Number.isFinite(v)) {
      pen = false;
      return;
    }
    d += `${pen ? "L" : "M"}${x(i).toFixed(1)} ${y(v).toFixed(1)}`;
    pen = true;
  });
  const last = [...values].reverse().findIndex((v) => typeof v === "number" && Number.isFinite(v));
  const lastIndex = last >= 0 ? values.length - 1 - last : -1;
  return (
    <svg className="df2-spark" width={W} height={H} viewBox={`0 0 ${W} ${H}`} aria-hidden="true" focusable="false">
      <path className="df2-spark-line" d={d} />
      {lastIndex >= 0 ? <circle className="df2-spark-dot" cx={x(lastIndex)} cy={y(values[lastIndex] as number)} r="2" /> : null}
    </svg>
  );
}

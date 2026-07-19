import { useEffect, useRef, useState } from "react";
import { DtIcon } from "../DtIcon";
import type { DayThroughput, JobStatusSlice } from "../../lib/overviewAnalytics";

const CHART_HEIGHT = 200;

interface ThroughputChartProps {
  series: DayThroughput[];
}

/** Zero-state chart — keeps axes/grid so the dashboard still reads as analytics-first. */
export function ThroughputChartPlaceholder({ series }: ThroughputChartProps) {
  const zeroSeries = series.map((s) => ({ ...s, rows: 0 }));
  return (
    <div className="df2-chart-placeholder-wrap">
      <ThroughputChart series={zeroSeries} />
      <p className="df2-chart-placeholder-caption">No throughput yet — completed transfers fill this chart.</p>
    </div>
  );
}

function formatAxisValue(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

/** Responsive SVG throughput — fills card width */
export function ThroughputChart({ series }: ThroughputChartProps) {
  const wrapRef = useRef<HTMLElement>(null);
  const [width, setWidth] = useState(360);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    const measure = () => {
      const w = el.getBoundingClientRect().width;
      if (w > 0) setWidth(Math.round(w));
    };
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const h = CHART_HEIGHT;
  const pad = { t: 16, r: 12, b: 32, l: 44 };
  const innerW = Math.max(width - pad.l - pad.r, 1);
  const innerH = h - pad.t - pad.b;
  const maxRows = Math.max(...series.map((s) => s.rows), 1);
  const slotW = innerW / Math.max(series.length, 1);
  const barW = Math.min(Math.max(slotW - 10, 6), 36);

  const points = series.map((s, i) => {
    const x = pad.l + slotW * i + slotW / 2;
    const y = pad.t + innerH - (s.rows / maxRows) * innerH;
    return { x, y, ...s };
  });

  const areaPath = points.length
    ? `M ${points[0].x} ${pad.t + innerH} L ${points.map((p) => `${p.x} ${p.y}`).join(" L ")} L ${points[points.length - 1].x} ${pad.t + innerH} Z`
    : "";

  const gridLines = [0, 0.5, 1].map((g) => pad.t + innerH * (1 - g));

  return (
    <figure ref={wrapRef} className="df2-chart df2-chart-throughput" aria-label="Throughput last 7 days">
      <svg
        width={width}
        height={h}
        viewBox={`0 0 ${width} ${h}`}
        className="df2-chart-svg df2-chart-svg-fill"
        role="img"
      >
        <defs>
          <linearGradient id="df-throughput-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#14b8a6" stopOpacity="0.28" />
            <stop offset="100%" stopColor="#14b8a6" stopOpacity="0.02" />
          </linearGradient>
          <linearGradient id="df-bar-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#0f766e" />
            <stop offset="100%" stopColor="#134e4a" />
          </linearGradient>
        </defs>
        {gridLines.map((y, i) => (
          <g key={y}>
            <line x1={pad.l} x2={width - pad.r} y1={y} y2={y} className="df2-chart-grid" />
            <text x={pad.l - 8} y={y + 4} className="df2-chart-axis" textAnchor="end">
              {formatAxisValue(maxRows * (1 - i * 0.5))}
            </text>
          </g>
        ))}
        {areaPath && <path d={areaPath} fill="url(#df-throughput-fill)" />}
        {series.map((s, i) => {
          const cx = pad.l + slotW * i + slotW / 2;
          const barH = (s.rows / maxRows) * innerH;
          const y = pad.t + innerH - barH;
          return (
            <rect
              key={s.label}
              x={cx - barW / 2}
              y={y}
              width={barW}
              height={Math.max(barH, s.rows > 0 ? 3 : 0)}
              rx={4}
              fill="url(#df-bar-fill)"
              className="df2-chart-bar"
            />
          );
        })}
        {points.map((p) => (
          <text key={p.label} x={p.x} y={h - 10} className="df2-chart-label" textAnchor="middle">
            {p.label}
          </text>
        ))}
      </svg>
    </figure>
  );
}

interface StatusDonutProps {
  slices: JobStatusSlice[];
  centerLabel: string;
  centerValue: string;
}

export function StatusDonut({ slices, centerLabel, centerValue }: StatusDonutProps) {
  const total = slices.reduce((s, x) => s + x.count, 0) || 1;
  const r = 46;
  const cx = 56;
  const cy = 56;
  let angle = -90;

  const arcs = slices.map((slice) => {
    const sweep = (slice.count / total) * 360;
    const start = angle;
    angle += sweep;
    const end = angle;
    const large = sweep > 180 ? 1 : 0;
    const rad = (deg: number) => (deg * Math.PI) / 180;
    const x1 = cx + r * Math.cos(rad(start));
    const y1 = cy + r * Math.sin(rad(start));
    const x2 = cx + r * Math.cos(rad(end));
    const y2 = cy + r * Math.sin(rad(end));
    const d = sweep >= 359.9
      ? `M ${cx - r} ${cy} A ${r} ${r} 0 1 1 ${cx + r} ${cy} A ${r} ${r} 0 1 1 ${cx - r} ${cy}`
      : `M ${cx} ${cy} L ${x1} ${y1} A ${r} ${r} 0 ${large} 1 ${x2} ${y2} Z`;
    return { ...slice, d };
  });

  return (
    <figure className="df2-chart df2-chart-donut df2-chart-donut-layout" aria-label={`${centerLabel}: ${centerValue}`}>
      <div className="df2-chart-donut-visual">
        <svg viewBox="0 0 112 112" className="df2-chart-svg df2-chart-donut-svg" role="img">
          <circle cx={cx} cy={cy} r={r + 5} className="df2-donut-shadow" />
          {arcs.map((a) => (
            <path key={a.key} d={a.d} fill={a.color} className="df2-donut-slice" />
          ))}
          <circle cx={cx} cy={cy} r={30} className="df2-donut-hole" />
          <text x={cx} y={cy - 3} textAnchor="middle" className="df2-donut-val">{centerValue}</text>
          <text x={cx} y={cy + 13} textAnchor="middle" className="df2-donut-lbl">{centerLabel}</text>
        </svg>
      </div>
      <ul className="df2-chart-legend">
        {slices.map((s) => (
          <li key={s.key}>
            <span className="df2-legend-swatch" style={{ background: s.color }} />
            <span className="df2-legend-label">{s.label}</span>
            <strong>{s.count}</strong>
          </li>
        ))}
      </ul>
    </figure>
  );
}

interface MetricGlassTileProps {
  label: string;
  value: string | number;
  sub?: string;
  icon: string;
  sparkline?: number[];
  tone?: "default" | "teal" | "green" | "amber";
  /** 0–100 progress ring in the accent well (e.g. success rate). */
  ring?: number | null;
}

export function MetricGlassTile({
  label,
  value,
  sub,
  icon,
  sparkline,
  tone = "default",
  ring,
}: MetricGlassTileProps) {
  const w = 72;
  const h = 24;
  const pts = sparkline?.length
    ? sparkline.map((v, i) => {
        const x = (i / Math.max(sparkline.length - 1, 1)) * w;
        const y = h - v * (h - 4) - 2;
        return `${x},${y}`;
      }).join(" ")
    : "";
  const ringPct = ring != null && Number.isFinite(ring) ? Math.max(0, Math.min(100, ring)) : null;
  const r = 14;
  const circ = 2 * Math.PI * r;

  return (
    <article className={`df2-metric-glass df2-metric-glass-${tone}${ringPct != null ? " has-ring" : ""}${pts ? " has-spark" : ""}`}>
      <header className="df2-metric-glass-head">
        <span className="df2-metric-glass-label">{label}</span>
        {ringPct != null ? (
          <span className="df2-metric-glass-ring" aria-hidden>
            <svg viewBox="0 0 36 36">
              <circle className="df2-metric-glass-ring-track" cx="18" cy="18" r={r} />
              <circle
                className="df2-metric-glass-ring-fill"
                cx="18"
                cy="18"
                r={r}
                strokeDasharray={`${(ringPct / 100) * circ} ${circ}`}
                transform="rotate(-90 18 18)"
              />
            </svg>
            <DtIcon name={icon} size={13} />
          </span>
        ) : (
          <span className="df2-metric-glass-icon" aria-hidden>
            <DtIcon name={icon} size={15} />
          </span>
        )}
      </header>
      <div className="df2-metric-glass-body">
        <strong className="df2-metric-glass-value">{value}</strong>
        {sub && <span className="df2-metric-glass-sub">{sub}</span>}
      </div>
      {pts && (
        <svg className="df2-metric-spark" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" aria-hidden>
          <polyline points={pts} fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      )}
    </article>
  );
}

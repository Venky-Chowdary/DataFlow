/**
 * Loader system — three primitives, one job each:
 *
 * - `Spinner`: a 14/16/20/32 px ring in `currentColor`, for buttons and inline
 *   "working…" text. Never the brand mark.
 * - `Skeleton` (+ presets): shape-matched placeholders for lists, tables,
 *   tiles and panels while their data loads.
 * - Real progress (determinate bars / steppers) for long operations lives with
 *   the feature; this module has no fake progress.
 *
 * The animated brand mark is reserved for the cold app boot (`AppBootOverlay`).
 */
import type { CSSProperties, ReactNode } from "react";
import { BrandLoader } from "./BrandLoader";

type SpinnerSize = "xs" | "sm" | "md" | "lg";

const SPINNER_PX: Record<SpinnerSize, number> = { xs: 14, sm: 16, md: 20, lg: 32 };

interface SpinnerProps {
  /** xs 14 (dense rows), sm 16 (buttons, inline text), md 20, lg 32 (panels). */
  size?: SpinnerSize;
  /**
   * Announced as a status when non-empty (default "Loading"). Pass "" when
   * visible text next to it already says what is happening, or inside a
   * button that sets aria-busy.
   */
  label?: string;
  className?: string;
  /** @deprecated No effect: there is one spinner. */
  premium?: boolean;
}

export function Spinner({ size = "md", label = "Loading", className = "" }: SpinnerProps) {
  const px = SPINNER_PX[size];
  const announced = label.trim().length > 0;
  return (
    <span
      className={`df-spinner df-spinner--${size} ${className}`.trim()}
      style={{ width: px, height: px }}
      role={announced ? "status" : undefined}
      aria-label={announced ? label : undefined}
      aria-hidden={announced ? undefined : true}
    >
      <svg viewBox="0 0 24 24" width={px} height={px} fill="none" aria-hidden="true" focusable="false">
        <circle className="df-spinner-track" cx="12" cy="12" r="9" />
        <path className="df-spinner-arc" d="M21 12a9 9 0 0 0-9-9" />
      </svg>
    </span>
  );
}

interface LoadingBlockProps {
  title?: string;
  hint?: string;
  size?: "sm" | "md" | "lg";
  /** Transparent glass — no card border; centered in section */
  variant?: "glass" | "card";
}

/** Centered loading state for a section that has no skeleton shape yet. */
export function LoadingBlock({
  title = "Loading",
  hint,
  size = "lg",
  variant = "glass",
}: LoadingBlockProps) {
  return (
    <div
      className={`dt-loading-block ${variant === "glass" ? "dt-loading-block--glass" : "dt-loading-block--card"}`}
      role="status"
      aria-live="polite"
      aria-busy="true"
    >
      <div className="dt-loading-block-visual">
        <Spinner size={size === "lg" ? "lg" : size === "md" ? "md" : "sm"} label="" />
      </div>
      <p className="dt-loading-block-title">{title}</p>
      {hint && <p className="dt-loading-block-hint">{hint}</p>}
    </div>
  );
}

/** Full-width centered loader for grid layouts (pipelines, catalogs) */
export function SectionLoader(props: LoadingBlockProps) {
  return (
    <div className="df-section-loader">
      <LoadingBlock {...props} variant="glass" />
    </div>
  );
}

interface PageLoaderProps {
  title?: string;
  hint?: string;
}

/** Full-page overlay while app data loads */
export function PageLoader({
  title = "Loading platform…",
  hint = "Connecting to your data plane and loading connectors.",
}: PageLoaderProps) {
  return <AppBootOverlay title={title} hint={hint} />;
}

/** Cold app boot only — the one place the animated brand mark appears. */
export function AppBootOverlay({
  title = "Loading workspace…",
  hint = "Connecting to your data plane and loading connectors.",
}: {
  title?: string;
  hint?: string;
}) {
  return (
    <div className="df-app-boot-overlay" role="status" aria-live="polite" aria-busy="true">
      <div className="df-app-boot-glass">
        <div className="dt-loading-block-visual dt-loading-block-visual--hero">
          <BrandLoader size={64} label={title} variant="premium" />
          <span className="dt-loading-block-pulse dt-loading-block-pulse--hero" aria-hidden />
        </div>
        <p className="df-app-boot-title">{title}</p>
        {hint && <p className="df-app-boot-hint">{hint}</p>}
        <div className="df-app-boot-progress" aria-hidden="true">
          <span />
        </div>
      </div>
    </div>
  );
}

/* ── Skeletons ─────────────────────────────────────────────────────────── */

interface SkeletonProps {
  className?: string;
  /** More than one draws a paragraph: full-width lines, the last at 70%. */
  lines?: number;
  /** `line` (12 px text bar), `block` (fills its box), `circle` (avatar/icon). */
  shape?: "line" | "block" | "circle";
  width?: number | string;
  height?: number | string;
}

export function Skeleton({ className = "", lines = 1, shape, width, height }: SkeletonProps) {
  if (lines > 1) {
    return (
      <div className={`dt-skeleton-stack ${className}`.trim()} aria-hidden>
        {Array.from({ length: lines }, (_, i) => (
          <div key={i} className="dt-skeleton dt-skeleton--line" style={{ width: i === lines - 1 ? "70%" : "100%" }} />
        ))}
      </div>
    );
  }
  const style: CSSProperties | undefined = width != null || height != null ? { width, height } : undefined;
  return <div className={`dt-skeleton${shape ? ` dt-skeleton--${shape}` : ""} ${className}`.trim()} style={style} aria-hidden />;
}

/** Wraps skeleton shapes in one polite status so screen readers hear "Loading …" once. */
function SkeletonRegion({ label, className, children }: { label: string; className: string; children: ReactNode }) {
  return (
    <div className={className} role="status" aria-busy="true" aria-live="polite">
      <span className="df2-sr-only">{label}</span>
      {children}
    </div>
  );
}

/** Table placeholder: a header bar and `rows` rows of `columns` cells. */
export function SkeletonTable({ rows = 5, columns = 4, label = "Loading table" }: { rows?: number; columns?: number; label?: string }) {
  const widths = ["72%", "48%", "60%", "36%", "54%", "40%"];
  return (
    <SkeletonRegion label={label} className="dt-skeleton-table">
      <div className="dt-skeleton-table-row is-head" aria-hidden style={{ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }}>
        {Array.from({ length: columns }, (_, c) => <Skeleton key={c} shape="line" width="44%" />)}
      </div>
      {Array.from({ length: rows }, (_, r) => (
        <div key={r} className="dt-skeleton-table-row" aria-hidden style={{ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }}>
          {Array.from({ length: columns }, (_, c) => <Skeleton key={c} shape="line" width={widths[(r + c) % widths.length]} />)}
        </div>
      ))}
    </SkeletonRegion>
  );
}

/** KPI / card tiles: a label line, a value bar and a caption line per tile. */
export function SkeletonTiles({ count = 4, label = "Loading", className = "" }: { count?: number; label?: string; className?: string }) {
  return (
    <SkeletonRegion label={label} className={`dt-skeleton-tiles ${className}`.trim()}>
      {Array.from({ length: count }, (_, i) => (
        <div key={i} className="dt-skeleton-tile" aria-hidden>
          <Skeleton shape="line" width="40%" />
          <Skeleton shape="block" width="56%" height={24} />
          <Skeleton shape="line" width="72%" />
        </div>
      ))}
    </SkeletonRegion>
  );
}

/** List rows: an icon, a title line and a meta line per row. */
export function SkeletonList({ rows = 5, label = "Loading", className = "" }: { rows?: number; label?: string; className?: string }) {
  return (
    <SkeletonRegion label={label} className={`dt-skeleton-list ${className}`.trim()}>
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className="dt-skeleton-list-row" aria-hidden>
          <Skeleton shape="circle" width={28} height={28} />
          <div className="dt-skeleton-list-text">
            <Skeleton shape="line" width={i % 2 ? "52%" : "64%"} />
            <Skeleton shape="line" width={i % 2 ? "34%" : "42%"} />
          </div>
        </div>
      ))}
    </SkeletonRegion>
  );
}

/**
 * Busy content for a button: spinner plus a visible label, so the control
 * keeps saying what it is doing ("Testing…"). The button sets aria-busy;
 * the label is its accessible name while busy. Without a label it is just the
 * spinner, for buttons that render their own text next to it.
 */
export function ButtonLoader({ label }: { label?: string }) {
  return (
    <span className="df-btn-busy">
      <Spinner size="sm" label="" />
      {label ? <span>{label}</span> : null}
    </span>
  );
}

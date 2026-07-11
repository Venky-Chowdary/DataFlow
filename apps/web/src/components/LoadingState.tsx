import { BrandLoader } from "./BrandLoader";

interface SpinnerProps {
  size?: "sm" | "md" | "lg";
  label?: string;
  className?: string;
  premium?: boolean;
}

export function Spinner({ size = "md", label, className = "", premium }: SpinnerProps) {
  const px = size === "sm" ? 18 : size === "lg" ? 56 : 36;
  return (
    <BrandLoader
      size={px}
      label={label ?? "Loading"}
      className={className}
      variant={premium || size === "lg" ? "premium" : "default"}
    />
  );
}

interface LoadingBlockProps {
  title?: string;
  hint?: string;
  size?: "sm" | "md" | "lg";
  /** Transparent glass — no card border; centered in section */
  variant?: "glass" | "card";
}

/** Centered loading state for cards, tables, grids, and sections */
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
        <Spinner size={size} premium />
        <span className="dt-loading-block-pulse" aria-hidden />
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

/** Centered full-application boot overlay — transparent glass, no card */
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

interface SkeletonProps {
  className?: string;
  lines?: number;
}

export function Skeleton({ className = "", lines = 1 }: SkeletonProps) {
  if (lines === 1) {
    return <div className={`dt-skeleton ${className}`.trim()} aria-hidden />;
  }
  return (
    <div className={`dt-skeleton-stack ${className}`.trim()} aria-hidden>
      {Array.from({ length: lines }, (_, i) => (
        <div key={i} className="dt-skeleton" style={{ width: i === lines - 1 ? "70%" : "100%" }} />
      ))}
    </div>
  );
}

/** Inline button loading — spinner + optional label */
export function ButtonLoader({ label }: { label?: string }) {
  return (
    <span className="dt-btn-loader" aria-hidden="true">
      <Spinner size="sm" label={label} />
      {label && <span>{label}</span>}
    </span>
  );
}

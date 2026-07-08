interface SpinnerProps {
  size?: "sm" | "md" | "lg";
  label?: string;
  className?: string;
}

export function Spinner({ size = "md", label, className = "" }: SpinnerProps) {
  return (
    <span className={`dt-spinner dt-spinner--${size} ${className}`.trim()} role="status" aria-label={label ?? "Loading"} />
  );
}

interface LoadingBlockProps {
  title?: string;
  hint?: string;
  size?: "sm" | "md" | "lg";
}

/** Centered loading state for cards, tables, and sections */
export function LoadingBlock({ title = "Loading", hint, size = "lg" }: LoadingBlockProps) {
  return (
    <div className="dt-loading-block">
      <Spinner size={size} />
      <p className="dt-loading-block-title">{title}</p>
      {hint && <p className="dt-loading-block-hint">{hint}</p>}
    </div>
  );
}

interface PageLoaderProps {
  title?: string;
}

/** Full-page skeleton while app data loads */
export function PageLoader({ title = "Loading platform…" }: PageLoaderProps) {
  return (
    <div className="dt-page-loader">
      <Spinner size="lg" />
      <p className="dt-page-loader-title">{title}</p>
      <div className="dt-page-loader-skeleton">
        <div className="dt-skeleton dt-skeleton-header" />
        <div className="dt-skeleton-grid">
          <div className="dt-skeleton dt-skeleton-stat" />
          <div className="dt-skeleton dt-skeleton-stat" />
          <div className="dt-skeleton dt-skeleton-stat" />
          <div className="dt-skeleton dt-skeleton-stat" />
        </div>
        <div className="dt-skeleton dt-skeleton-card" />
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

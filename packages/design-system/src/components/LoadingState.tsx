import { ProgressBar } from "./ProgressBar";

interface LoadingStateProps {
  label?: string;
  sublabel?: string;
  progress?: number;
  indeterminate?: boolean;
  compact?: boolean;
}

export function LoadingState({
  label = "Loading…",
  sublabel,
  progress,
  indeterminate = true,
  compact = false,
}: LoadingStateProps) {
  return (
    <div className={["df-loading", compact ? "df-loading--compact" : ""].filter(Boolean).join(" ")}>
      <div className="df-loading-ring" aria-hidden />
      <div className="df-loading-copy">
        <span className="df-loading-label">{label}</span>
        {sublabel && <span className="df-loading-sublabel">{sublabel}</span>}
      </div>
      {!compact && (
        <ProgressBar
          indeterminate={indeterminate && progress === undefined}
          value={progress}
          tone="brand"
          size="sm"
        />
      )}
    </div>
  );
}

interface SkeletonBlockProps {
  width?: string;
  height?: string;
  className?: string;
}

export function SkeletonBlock({ width = "100%", height = "16px", className = "" }: SkeletonBlockProps) {
  return (
    <span
      className={["df-skeleton", className].filter(Boolean).join(" ")}
      style={{ width, height }}
      aria-hidden
    />
  );
}

export function MetricSkeleton() {
  return (
    <div className="df-metric-row" aria-busy="true" aria-label="Loading metrics">
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i} className="df-metric-card df-metric-card--skeleton">
          <SkeletonBlock width="60%" height="12px" />
          <SkeletonBlock width="40%" height="28px" className="df-skeleton--value" />
        </div>
      ))}
    </div>
  );
}

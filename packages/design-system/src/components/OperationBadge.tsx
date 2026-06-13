interface OperationBadgeProps {
  label: string;
  sourceSummary: string;
  destinationSummary: string;
}

export function OperationBadge({ label, sourceSummary, destinationSummary }: OperationBadgeProps) {
  return (
    <div className="df-path-badge">
      <span className="df-path-badge-tag">{label}</span>
      <span>{sourceSummary}</span>
      <span className="df-path-arrow" aria-hidden>
        →
      </span>
      <span>{destinationSummary}</span>
    </div>
  );
}

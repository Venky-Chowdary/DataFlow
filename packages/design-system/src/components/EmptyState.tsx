interface EmptyStateProps {
  title: string;
  description?: string;
  action?: React.ReactNode;
}

export function EmptyState({ title, description, action }: EmptyStateProps) {
  return (
    <div className="df-empty-state">
      <div className="df-empty-state-icon" aria-hidden>
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
          <path d="M4 7h16M4 12h10M4 17h14" strokeLinecap="round" />
        </svg>
      </div>
      <div className="df-empty-state-title">{title}</div>
      {description && <p className="df-empty-state-desc">{description}</p>}
      {action}
    </div>
  );
}

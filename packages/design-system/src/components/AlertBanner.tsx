interface AlertBannerProps {
  variant: "danger" | "warning" | "info";
  title?: string;
  message: string;
  onRetry?: () => void;
}

export function AlertBanner({ variant, title, message, onRetry }: AlertBannerProps) {
  return (
    <div className={["df-alert-banner", `df-alert-banner--${variant}`].join(" ")} role="alert">
      <div>
        {title && <div className="df-alert-banner-title">{title}</div>}
        <div className="df-alert-banner-message">{message}</div>
      </div>
      {onRetry && (
        <button type="button" className="df-btn df-btn-ghost df-btn-sm" onClick={onRetry}>
          Retry
        </button>
      )}
    </div>
  );
}

interface TransferCtaProps {
  onClick: () => void;
  disabled?: boolean;
  loading?: boolean;
}

/** Primary one-click transfer control — distinctive, not generic gradient */
export function TransferCta({ onClick, disabled, loading }: TransferCtaProps) {
  return (
    <button
      type="button"
      className="df-transfer-cta"
      onClick={onClick}
      disabled={disabled || loading}
    >
      <span className="df-transfer-cta-icon" aria-hidden>
        <svg width="14" height="16" viewBox="0 0 14 16" fill="currentColor">
          <path d="M2 1.5L12 8L2 14.5V1.5Z" />
        </svg>
      </span>
      <span className="df-transfer-cta-label">{loading ? "Transferring…" : "Transfer"}</span>
    </button>
  );
}

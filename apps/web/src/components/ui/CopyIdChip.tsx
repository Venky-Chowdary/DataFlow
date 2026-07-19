import { useState, type MouseEvent } from "react";
import { DtIcon } from "../DtIcon";

interface CopyIdChipProps {
  id: string;
  /** Short label shown before the ID, e.g. "Job", "Run", "Pipeline". */
  label?: string;
  /** Optional callback after copy (e.g. feed Pilot context). */
  onCopied?: (id: string) => void;
  /** Compact truncated display with full ID on hover. */
  compact?: boolean;
  className?: string;
}

/**
 * Enterprise tracking chip — full/selectable ID + one-click copy for Data Pilot.
 */
export function CopyIdChip({
  id,
  label = "ID",
  onCopied,
  compact = false,
  className = "",
}: CopyIdChipProps) {
  const [copied, setCopied] = useState(false);
  if (!id) return null;

  const display = compact && id.length > 12 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id;

  const copy = async (e: MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    try {
      await navigator.clipboard.writeText(id);
      setCopied(true);
      onCopied?.(id);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      /* ignore */
    }
  };

  return (
    <span className={`df2-copy-id ${compact ? "is-compact" : ""} ${className}`.trim()} title={id}>
      <span className="df2-copy-id-label">{label}</span>
      <code className="df2-copy-id-value">{display}</code>
      <button type="button" className="df2-copy-id-btn" onClick={copy} aria-label={`Copy ${label} ${id}`}>
        <DtIcon name={copied ? "check" : "layers"} size={12} />
        {copied ? "Copied" : "Copy"}
      </button>
    </span>
  );
}

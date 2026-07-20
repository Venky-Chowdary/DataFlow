import { ConnectorIcon } from "../../app/brand-icons";
import { formatRouteRowCount } from "../../lib/formatRouteRowCount";

interface TransferRouteBarProps {
  sourceLabel: string;
  destLabel: string;
  sourceType?: string;
  destType?: string;
  rowCount?: number;
  live?: boolean;
}

export { formatRouteRowCount };

/** Compact source → destination strip — not a second stepper. */
export function TransferRouteBar({
  sourceLabel,
  destLabel,
  sourceType = "file",
  destType = "",
  rowCount,
  live = false,
}: TransferRouteBarProps) {
  const hasSource = sourceLabel !== "Choose source" && !sourceLabel.startsWith("Cloud source") && sourceLabel !== "Database source";
  const hasDest = destLabel !== "Choose destination" && Boolean(destType);

  if (!hasSource && !hasDest && !rowCount) return null;

  const countLabel = rowCount != null && rowCount > 0 ? formatRouteRowCount(rowCount) : null;

  return (
    <div className={`df2-route-bar ${live ? "df2-route-bar-live" : ""}`} aria-label="Current route">
      <div className={`df2-route-bar-endpoint${hasSource ? "" : " is-pending"}`}>
        <ConnectorIcon id={sourceType || "file"} size={18} />
        <span className="df2-route-bar-label" title={sourceLabel}>{sourceLabel}</span>
      </div>
      <span className="df2-route-bar-arrow" aria-hidden>→</span>
      <div className={`df2-route-bar-endpoint${hasDest ? "" : " is-pending"}`}>
        {hasDest ? (
          <ConnectorIcon id={destType} size={18} />
        ) : (
          <span className="df2-route-bar-placeholder-icon" aria-hidden />
        )}
        <span className="df2-route-bar-label" title={destLabel}>{hasDest ? destLabel : "Choose destination"}</span>
      </div>
      {countLabel && (
        <span className="df2-route-bar-meta" title={countLabel.full}>
          {countLabel.short}
        </span>
      )}
    </div>
  );
}

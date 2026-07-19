import { DtIcon } from "../DtIcon";
import type { JobNotificationResult } from "../../lib/types";

interface NotificationDeliveryStripProps {
  notifications?: JobNotificationResult[] | null;
  className?: string;
  /** Compact one-line for theater; default expands each channel. */
  compact?: boolean;
}

/**
 * Surfaces workspace notification dispatch results from the job document
 * (Slack / Teams / email / webhook / ServiceNow).
 */
export function NotificationDeliveryStrip({
  notifications,
  className = "",
  compact = false,
}: NotificationDeliveryStripProps) {
  const items = Array.isArray(notifications) ? notifications.filter(Boolean) : [];
  if (!items.length) return null;

  const okCount = items.filter((n) => n.ok).length;
  const failCount = items.length - okCount;

  return (
    <section
      className={`df2-notify-strip ${compact ? "is-compact" : ""} ${className}`.trim()}
      aria-label="Notification delivery"
    >
      <header className="df2-notify-strip-head">
        <DtIcon name="bell" size={14} />
        <strong>Notifications</strong>
        <span>
          {okCount} sent{failCount > 0 ? ` · ${failCount} failed` : ""}
        </span>
      </header>
      <ul className="df2-notify-strip-list">
        {items.map((n) => (
          <li
            key={`${n.channel_id}-${n.kind}`}
            className={`df2-notify-strip-item ${n.ok ? "is-ok" : "is-fail"}`}
          >
            <span className="df2-notify-kind">{n.kind}</span>
            <span className="df2-notify-status">
              {n.ok ? "Sent" : n.error || "Delivery failed"}
            </span>
            {n.channel_id ? (
              <span className="df2-notify-channel" title={n.channel_id}>
                {n.channel_id.length > 18 ? `${n.channel_id.slice(0, 16)}…` : n.channel_id}
              </span>
            ) : null}
          </li>
        ))}
      </ul>
    </section>
  );
}

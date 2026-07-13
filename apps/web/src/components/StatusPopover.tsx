import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { DtIcon } from "./DtIcon";
import type { Screen } from "../lib/types";

interface StatusPopoverProps {
  apiOnline: boolean;
  failedJobsCount: number;
  runningJobsCount: number;
  unhealthyConnectorsCount: number;
  onNavigate: (screen: Screen) => void;
}

export function StatusPopover({
  apiOnline,
  failedJobsCount,
  runningJobsCount,
  unhealthyConnectorsCount,
  onNavigate,
}: StatusPopoverProps) {
  const [open, setOpen] = useState(false);
  const [coords, setCoords] = useState<{ top: number; right: number } | null>(null);
  const anchorRef = useRef<HTMLDivElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);

  const hasAttentionItems = !apiOnline || failedJobsCount > 0 || unhealthyConnectorsCount > 0;
  const systemTone: "ok" | "live" | "warn" = hasAttentionItems ? "warn" : runningJobsCount > 0 ? "live" : "ok";

  const parts: string[] = [];
  if (!apiOnline) {
    parts.push("Control plane API is offline. Transfers and validation checks can fail until connectivity recovers.");
  } else {
    if (failedJobsCount > 0) {
      parts.push(`${failedJobsCount} failed job${failedJobsCount > 1 ? "s" : ""} need triage in Job Theater.`);
    }
    if (unhealthyConnectorsCount > 0) {
      parts.push(`${unhealthyConnectorsCount} connector${unhealthyConnectorsCount > 1 ? "s" : ""} need health checks before production transfers.`);
    }
    if (runningJobsCount > 0 && parts.length === 0) {
      parts.push(`${runningJobsCount} active transfer${runningJobsCount > 1 ? "s" : ""} streaming now.`);
    }
    if (parts.length === 0) {
      parts.push("All systems healthy. Preflight, routing, and transfer services are operational.");
    }
  }
  const systemMessage = parts.join(" ");

  const updatePosition = () => {
    const el = anchorRef.current;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    setCoords({
      top: rect.bottom + 10,
      right: Math.max(12, window.innerWidth - rect.right),
    });
  };

  useLayoutEffect(() => {
    if (!open) return;
    updatePosition();
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      const target = e.target as Node;
      if (anchorRef.current?.contains(target) || panelRef.current?.contains(target)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    const onReposition = () => updatePosition();
    document.addEventListener("mousedown", onClick);
    document.addEventListener("keydown", onKey);
    window.addEventListener("resize", onReposition);
    window.addEventListener("scroll", onReposition, true);
    return () => {
      document.removeEventListener("mousedown", onClick);
      document.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", onReposition);
      window.removeEventListener("scroll", onReposition, true);
    };
  }, [open]);

  const navigate = (screen: Screen) => {
    setOpen(false);
    onNavigate(screen);
  };

  return (
    <div ref={anchorRef} className="df2-status-popover-anchor">
      <button
        type="button"
        className={`df2-btn df2-status-trigger ${systemTone}${hasAttentionItems ? " attention" : ""}`}
        onClick={() => setOpen((o) => !o)}
        aria-label="System status"
        title="System status"
        aria-expanded={open}
      >
        <DtIcon name={systemTone === "ok" ? "check" : systemTone === "warn" ? "alert" : "activity"} size={16} />
        <span className="df2-topbar-btn-text">Status</span>
        {hasAttentionItems && (
          <span className="df2-status-trigger-badge" aria-hidden="true">
            {failedJobsCount + unhealthyConnectorsCount > 0 ? failedJobsCount + unhealthyConnectorsCount : ""}
          </span>
        )}
      </button>

      {open && coords && createPortal(
        <div
          ref={panelRef}
          className={`df2-status-popover ${systemTone}`}
          role="dialog"
          aria-label="System status"
          style={{ top: coords.top, right: coords.right }}
        >
          <div className="df2-status-popover-head">
            <span className={`df2-status-popover-icon ${systemTone}`} aria-hidden>
              <DtIcon
                name={systemTone === "ok" ? "check" : systemTone === "warn" ? "alert" : "activity"}
                size={18}
              />
            </span>
            <div>
              <strong>System status</strong>
              <p>{systemMessage}</p>
            </div>
          </div>

          <div className="df2-status-popover-metrics">
            <span className={`df2-status-metric-pill ${!apiOnline ? "warn" : "ok"}`}>
              API {apiOnline ? "online" : "offline"}
            </span>
            <span className={`df2-status-metric-pill ${failedJobsCount > 0 ? "warn" : "ok"}`}>
              Failed {failedJobsCount}
            </span>
            <span className={`df2-status-metric-pill ${runningJobsCount > 0 ? "live" : "ok"}`}>
              Running {runningJobsCount}
            </span>
            <span className={`df2-status-metric-pill ${unhealthyConnectorsCount > 0 ? "warn" : "ok"}`}>
              Connector alerts {unhealthyConnectorsCount}
            </span>
          </div>

          {(failedJobsCount > 0 || unhealthyConnectorsCount > 0) && (
            <div className="df2-status-popover-actions">
              {failedJobsCount > 0 && (
                <button type="button" className="df2-btn df2-btn-sm" onClick={() => navigate("jobs")}>
                  Open Job Theater
                </button>
              )}
              {unhealthyConnectorsCount > 0 && (
                <button type="button" className="df2-btn df2-btn-sm" onClick={() => navigate("connectors")}>
                  Review connectors
                </button>
              )}
            </div>
          )}
        </div>,
        document.body,
      )}
    </div>
  );
}

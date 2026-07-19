import { ReactNode, useEffect, useRef } from "react";
import { createPortal } from "react-dom";
import { DtIcon } from "../DtIcon";

interface DrawerProps {
  open: boolean;
  onClose: () => void;
  title?: ReactNode;
  subtitle?: ReactNode;
  /** Rendered before the title (e.g. a connector icon). */
  icon?: ReactNode;
  /** Rendered on the title row after the title (e.g. status badges). */
  headerExtra?: ReactNode;
  /** Sticky footer content (e.g. primary/secondary actions). */
  footer?: ReactNode;
  /** Panel width in px on desktop. Defaults to 560. */
  width?: number;
  side?: "right" | "left";
  ariaLabel?: string;
  className?: string;
  children: ReactNode;
}

/**
 * Reusable slide-over Drawer primitive: portal-rendered overlay + side panel
 * with Escape-to-close, body scroll lock, and focus handoff. Shares the modal
 * design tokens (df2-drawer-*) so it stays consistent with df2-modal.
 */
export function Drawer({
  open,
  onClose,
  title,
  subtitle,
  icon,
  headerExtra,
  footer,
  width = 560,
  side = "right",
  ariaLabel,
  className = "",
  children,
}: DrawerProps) {
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const t = window.requestAnimationFrame(() => panelRef.current?.focus());
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
      window.cancelAnimationFrame(t);
    };
  }, [open, onClose]);

  if (!open) return null;

  return createPortal(
    <div className="df2-drawer-overlay" role="presentation" onClick={onClose}>
      <div
        ref={panelRef}
        className={`df2-drawer df2-drawer-${side} ${className}`}
        style={{ width: `min(${width}px, 96vw)` }}
        role="dialog"
        aria-modal="true"
        aria-label={ariaLabel}
        tabIndex={-1}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="df2-drawer-header">
          <div className="df2-drawer-header-main">
            {icon && <span className="df2-drawer-icon" aria-hidden>{icon}</span>}
            <div className="df2-drawer-heading">
              <div className="df2-drawer-title-row">
                {title && <h2 className="df2-drawer-title">{title}</h2>}
                {headerExtra}
              </div>
              {subtitle && <p className="df2-drawer-subtitle">{subtitle}</p>}
            </div>
          </div>
          <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-drawer-close" onClick={onClose} aria-label="Close">
            <DtIcon name="x" size={16} />
          </button>
        </div>

        <div className="df2-drawer-body">{children}</div>

        {footer && <div className="df2-drawer-footer">{footer}</div>}
      </div>
    </div>,
    document.body,
  );
}

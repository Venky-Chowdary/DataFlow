import { ReactNode, useEffect, useRef } from "react";
import { createPortal } from "react-dom";
import { DtIcon } from "../DtIcon";

export type DrawerSize = "md" | "lg" | "xl" | "full";

const DRAWER_WIDTH: Record<DrawerSize, number> = {
  md: 560,
  lg: 720,
  xl: 960,
  full: 1400,
};

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
  /** Panel width in px on desktop. Prefer `size` for viewport-aware panels. */
  width?: number;
  /** Viewport-aware width: md 560 · lg 720 · xl 960 · full ~1200 (uses available height). */
  size?: DrawerSize;
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
  width,
  size = "md",
  side = "right",
  ariaLabel,
  className = "",
  children,
}: DrawerProps) {
  const panelRef = useRef<HTMLDivElement>(null);
  const resolvedWidth = width ?? DRAWER_WIDTH[size];

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
        className={`df2-drawer df2-drawer-${side} df2-drawer-size-${size} ${className}`}
        style={{
          width: size === "full"
            ? `min(${resolvedWidth}px, 92vw)`
            : `min(${resolvedWidth}px, 96vw)`,
          maxWidth: size === "full" ? "92vw" : undefined,
        }}
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

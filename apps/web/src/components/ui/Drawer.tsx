import { ReactNode, useEffect, useRef } from "react";
import { createPortal } from "react-dom";
import { DtIcon } from "../DtIcon";
import { lockBodyScroll } from "../../lib/bodyScrollLock";

/**
 * Canonical right-rail width for every app slide-over (Transfer Studio, Jobs,
 * Connectors, Contracts, Pipelines). One size — never invent per-page widths.
 */
export const DRAWER_PANEL_WIDTH_PX = 720;

export type DrawerSize = "lg" | "full";

const DRAWER_WIDTH: Record<DrawerSize, number> = {
  lg: DRAWER_PANEL_WIDTH_PX,
  /** Rare wide evidence panels — prefer `lg` unless content truly needs it. */
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
  /**
   * @deprecated Ignored. All right drawers use `DRAWER_PANEL_WIDTH_PX` via `size="lg"`.
   */
  width?: number;
  /**
   * `lg` (720px) is the app-wide default. Use `full` only for rare wide evidence.
   * Legacy aliases `md` / `xl` coerce to `lg`.
   */
  size?: DrawerSize | "md" | "xl";
  side?: "right" | "left";
  ariaLabel?: string;
  className?: string;
  children: ReactNode;
}

function resolveSize(size: DrawerProps["size"]): DrawerSize {
  if (size === "full") return "full";
  return "lg";
}

/**
 * Single reusable slide-over for the whole app. Portal + Escape + scroll lock.
 * Prefer this over page-local drawers so width/theme stay identical everywhere.
 */
export function Drawer({
  open,
  onClose,
  title,
  subtitle,
  icon,
  headerExtra,
  footer,
  size = "lg",
  side = "right",
  ariaLabel,
  className = "",
  children,
}: DrawerProps) {
  const panelRef = useRef<HTMLDivElement>(null);
  const resolvedSize = resolveSize(size);
  const resolvedWidth = DRAWER_WIDTH[resolvedSize];

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    const unlock = lockBodyScroll();
    const t = window.requestAnimationFrame(() => panelRef.current?.focus());
    return () => {
      document.removeEventListener("keydown", onKey);
      unlock();
      window.cancelAnimationFrame(t);
    };
  }, [open, onClose]);

  if (!open) return null;

  return createPortal(
    <div className="df2-drawer-overlay" role="presentation" onClick={onClose}>
      <div
        ref={panelRef}
        className={`df2-drawer df2-drawer-${side} df2-drawer-size-${resolvedSize} ${className}`}
        style={{
          width: resolvedSize === "full"
            ? `min(${resolvedWidth}px, 92vw)`
            : `min(${resolvedWidth}px, 96vw)`,
          maxWidth: resolvedSize === "full" ? "92vw" : undefined,
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

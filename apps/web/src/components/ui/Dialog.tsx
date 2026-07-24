import { ReactNode, useEffect, useRef } from "react";
import { createPortal } from "react-dom";
import { DtIcon } from "../DtIcon";
import { lockBodyScroll } from "../../lib/bodyScrollLock";

interface DialogProps {
  open: boolean;
  onClose: () => void;
  title?: ReactNode;
  subtitle?: ReactNode;
  footer?: ReactNode;
  /** Visual size for workspace dialogs. Prefer `full` for Map / proof / expanders so panels match. Confirm stays `md`. */
  size?: "md" | "lg" | "xl" | "full";
  ariaLabel?: string;
  className?: string;
  children: ReactNode;
}

/**
 * Centered dialog using shared df2-modal tokens (Escape, overlay click, scroll lock).
 * Studio expanders (mapping table, structure preview, proof) should use size="full".
 */
export function Dialog({
  open,
  onClose,
  title,
  subtitle,
  footer,
  size = "full",
  ariaLabel,
  className = "",
  children,
}: DialogProps) {
  const panelRef = useRef<HTMLDivElement>(null);

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

  const sizeClass =
    size === "full"
      ? "df2-modal-full"
      : size === "xl"
        ? "df2-modal-xl"
        : size === "lg"
          ? "df2-modal-lg"
          : "";

  return createPortal(
    <div className="df2-modal-overlay" role="presentation" onClick={onClose}>
      <div
        ref={panelRef}
        className={`df2-modal ${sizeClass} ${className}`.trim()}
        role="dialog"
        aria-modal="true"
        aria-labelledby={title ? "df2-dialog-title" : undefined}
        aria-label={title ? undefined : ariaLabel}
        tabIndex={-1}
        onClick={(e) => e.stopPropagation()}
      >
        {(title || subtitle) && (
          <div className="df2-modal-header">
            <div>
              {title && <h2 id="df2-dialog-title" className="df2-modal-title">{title}</h2>}
              {subtitle && <p className="df2-modal-subtitle">{subtitle}</p>}
            </div>
            <button
              type="button"
              className="df2-btn df2-btn-ghost df2-btn-sm"
              onClick={onClose}
              aria-label="Close"
            >
              <DtIcon name="x" size={16} />
            </button>
          </div>
        )}
        <div className="df2-modal-body">{children}</div>
        {footer && <div className="df2-modal-footer">{footer}</div>}
      </div>
    </div>,
    document.body,
  );
}

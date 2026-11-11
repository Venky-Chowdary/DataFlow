import { ReactNode, useEffect, useId, useRef } from "react";
import { createPortal } from "react-dom";
import { isTopOverlay, pushOverlay } from "../../lib/overlayStack";
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
  restoreFocusTo?: HTMLElement | null;
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
  restoreFocusTo,
}: DialogProps) {
  const panelRef = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const closeRef = useRef(onClose);

  useEffect(() => { closeRef.current = onClose; }, [onClose]);

  useEffect(() => {
    if (!open) return;
    const previousFocus = restoreFocusTo
      ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    const onKey = (e: KeyboardEvent) => {
      const panel = panelRef.current;
      // The most recently opened overlay owns Escape and Tab (open order, not
      // DOM order: a drawer in the app shell precedes <body> portals).
      if (!panel || !isTopOverlay(panel)) return;
      if (e.key === "Escape") {
        e.preventDefault();
        closeRef.current();
      } else if (e.key === "Tab") {
        const controls = Array.from(panel.querySelectorAll<HTMLElement>(
          'button:not([disabled]), a[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])',
        )).filter((element) => {
          if (element.tabIndex < 0 || element.closest('[hidden], [inert], [aria-hidden="true"]')) return false;
          let ancestor: HTMLElement | null = element;
          while (ancestor && ancestor !== panel) {
            const style = window.getComputedStyle(ancestor);
            if (style.display === "none" || style.visibility === "hidden") return false;
            if (ancestor.tagName === "DETAILS" && !(ancestor as HTMLDetailsElement).open) {
              const summary = ancestor.querySelector(":scope > summary");
              if (!summary?.contains(element)) return false;
            }
            ancestor = ancestor.parentElement;
          }
          return true;
        });
        const first = controls[0];
        const last = controls.at(-1);
        if (!first || !last) {
          e.preventDefault();
          panel.focus();
        } else if (!panel.contains(document.activeElement) || document.activeElement === panel) {
          e.preventDefault();
          (e.shiftKey ? last : first).focus();
        } else if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    };
    document.addEventListener("keydown", onKey);
    const unlock = lockBodyScroll();
    const t = window.requestAnimationFrame(() => {
      if (!panelRef.current?.contains(document.activeElement)) panelRef.current?.focus();
    });
    const popOverlay = panelRef.current ? pushOverlay(panelRef.current) : () => {};
    return () => {
      popOverlay();
      document.removeEventListener("keydown", onKey);
      unlock();
      window.cancelAnimationFrame(t);
      if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true });
    };
  }, [open]);

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
        aria-labelledby={title ? titleId : undefined}
        aria-label={title ? undefined : ariaLabel}
        tabIndex={-1}
        onClick={(e) => e.stopPropagation()}
      >
        {(title || subtitle) && (
          <div className="df2-modal-header">
            <div>
              {title && <h2 id={titleId} className="df2-modal-title">{title}</h2>}
              {subtitle && <p className="df2-modal-subtitle">{subtitle}</p>}
            </div>
            <button
              type="button"
              className="df2-close-btn"
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

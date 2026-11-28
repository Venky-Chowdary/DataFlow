import { createContext, useCallback, useContext, useMemo, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { Toaster, toast as sonner } from "sonner";
import { DtIcon, type IconName } from "./DtIcon";
import { Dialog } from "./ui/Dialog";
import { ErrorReferenceChip } from "./ErrorReferenceChip";
import { splitReference } from "../lib/errorCopy";
import { toastFingerprint, type ToastTone } from "../lib/toastDedupe";

export type { ToastTone };

export interface ToastItem {
  id: string;
  key: string;
  title: string;
  message?: string;
  tone: ToastTone;
  createdAt: number;
}

interface ToastContextValue {
  toast: (opts: { title: string; message?: string; tone?: ToastTone }) => void;
  dismiss: (id: string) => void;
  dismissMatching: (title: string) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

/** Standard status glyphs: the same four everywhere a tone is shown. */
export const TOAST_TONE_ICON: Record<ToastTone, IconName> = {
  info: "info",
  success: "success",
  warning: "warning",
  error: "error",
};

const TONE_LABEL: Record<ToastTone, string> = {
  info: "Information",
  success: "Success",
  warning: "Warning",
  error: "Error",
};

/**
 * How long a toast stays when nobody is looking at it. Hovering the stack,
 * focusing a toast or pressing Alt+T pauses every timer (WCAG 2.2.1). Errors
 * stay until the user dismisses them: the user has to act on them.
 */
export const TOAST_HOLD_MS: Record<ToastTone, number> = {
  info: 6000,
  success: 6000,
  warning: 10000,
  error: Infinity,
};

/** Longer than this, the toast clamps and offers "Details" with the full text. */
const DETAILS_AFTER_CHARS = 180;

/**
 * Sonner pauses on hover and on Alt+T but not when keyboard focus lands on a
 * toast. Its list listens for mouse enter/leave, so focus is translated into
 * those events on the list itself: one pause model for pointer and keyboard.
 */
function pauseWhileFocused(event: React.FocusEvent<HTMLElement>, entering: boolean) {
  const list = event.currentTarget.closest("[data-sonner-toaster]");
  if (!list) return;
  if (!entering && list.contains(event.relatedTarget as Node | null)) return;
  const init = { bubbles: true, relatedTarget: entering ? null : document.body } as MouseEventInit;
  list.dispatchEvent(new MouseEvent(entering ? "mousemove" : "mouseout", init));
}

function ToastCard({
  item,
  onDetails,
  onDismiss,
}: {
  item: { title: string; message?: string; tone: ToastTone };
  onDetails: () => void;
  onDismiss: () => void;
}) {
  const { text, reference } = splitReference(item.message);
  const long = text.length > DETAILS_AFTER_CHARS;
  return (
    <div
      className={`dt-toast dt-toast--${item.tone}`}
      onFocus={(event) => pauseWhileFocused(event, true)}
      onBlur={(event) => pauseWhileFocused(event, false)}
    >
      <span className="dt-toast-icon">
        <DtIcon name={TOAST_TONE_ICON[item.tone]} size={20} chrome="product" />
        <span className="df2-sr-only">{TONE_LABEL[item.tone]}: </span>
      </span>
      <div className="dt-toast-body">
        <strong className="dt-toast-title">{item.title}</strong>
        {text && <p className={`dt-toast-message${long ? " is-clamped" : ""}`}>{text}</p>}
        {(reference || long) && (
          <div className="dt-toast-actions">
            {reference && <ErrorReferenceChip reference={reference} />}
            {long && (
              <button type="button" className="dt-toast-action" onClick={onDetails} aria-haspopup="dialog">
                Details
              </button>
            )}
          </div>
        )}
      </div>
      <button type="button" className="dt-toast-close" onClick={onDismiss} aria-label="Dismiss notification">
        <DtIcon name="x" size={16} chrome="product" />
      </button>
    </div>
  );
}

const TOASTER_STYLE = {
  zIndex: "var(--df-z-toast)",
  "--width": "var(--df-toast-width)",
} as CSSProperties;

/**
 * App notifications: one fixed region (bottom-right, bottom-centre on phones),
 * never in the page flow. At most three show; more stack behind and expand on
 * hover. Identical notifications collapse into one and restart its timer.
 */
export function ToastProvider({ children }: { children: ReactNode }) {
  const [review, setReview] = useState<ToastItem[]>([]);
  const live = useRef(new Map<string, ToastItem>());
  const openReview = useRef<(item: ToastItem) => void>(() => {});

  const dismiss = useCallback((id: string) => {
    live.current.delete(id);
    sonner.dismiss(id);
  }, []);

  /** Show or update one toast; `Infinity` holds it (while its details are open). */
  const show = useCallback((item: ToastItem, duration: number) => {
    live.current.set(item.id, item);
    const forget = () => { live.current.delete(item.id); };
    sonner.custom(
      () => <ToastCard item={item} onDismiss={() => dismiss(item.id)} onDetails={() => openReview.current(item)} />,
      { id: item.id, duration, onDismiss: forget, onAutoClose: forget },
    );
  }, [dismiss]);

  openReview.current = (item: ToastItem) => {
    show(item, Infinity);
    setReview([item]);
    // When focus leaves its region, sonner hands it back to whatever was
    // focused before the toast, which pulls it straight out of the review the
    // moment the dialog takes it. Take it back once that hand-back is done.
    requestAnimationFrame(() => requestAnimationFrame(() => {
      const panel = document.querySelector<HTMLElement>(".dt-toast-review");
      if (panel && !panel.contains(document.activeElement)) panel.focus();
    }));
  };

  const closeReview = useCallback(() => {
    // The toast resumes its timer once its details are closed.
    for (const item of review) if (live.current.has(item.id)) show(item, TOAST_HOLD_MS[item.tone]);
    setReview([]);
  }, [review, show]);

  const dismissMatching = useCallback((title: string) => {
    const match = title.trim();
    for (const [id, item] of live.current) if (item.title === match) dismiss(id);
  }, [dismiss]);

  const toast = useCallback(
    ({ title, message, tone = "info" }: { title: string; message?: string; tone?: ToastTone }) => {
      const cleanTitle = title.trim();
      const cleanMessage = message?.trim() || undefined;
      const key = toastFingerprint({ title: cleanTitle, message: cleanMessage, tone });
      show({ id: key, key, title: cleanTitle, message: cleanMessage, tone, createdAt: Date.now() }, TOAST_HOLD_MS[tone]);
    },
    [show],
  );

  const value = useMemo(() => ({ toast, dismiss, dismissMatching }), [toast, dismiss, dismissMatching]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      {/* On <body>: a transformed or clipped page ancestor can never move or cut the region. */}
      {createPortal(
        <Toaster
          position="bottom-right"
          visibleToasts={3}
          gap={8}
          offset={{ bottom: "var(--df-toast-offset-bottom, 20px)", right: 20 }}
          mobileOffset={{ bottom: "var(--df-toast-offset-bottom-mobile, 16px)", left: 16, right: 16 }}
          containerAriaLabel="Notifications"
          style={TOASTER_STYLE}
          toastOptions={{ unstyled: true, className: "dt-toast-item" }}
        />,
        document.body,
      )}
      <Dialog open={review.length > 0} onClose={closeReview} title="Notification details" size="md" className="dt-toast-review">
        {review.map((item) => (
          <section key={item.id} className={`dt-toast-review-item dt-toast-review-item--${item.tone}`}>
            <h3>{item.title}</h3>
            {item.message && <p>{splitReference(item.message).text}</p>}
            {item.message && <ErrorReferenceChip reference={splitReference(item.message).reference} />}
          </section>
        ))}
      </Dialog>
    </ToastContext.Provider>
  );
}

export function useToast() {
  const ctx = useContext(ToastContext);
  if (!ctx) {
    if ((import.meta as { env?: { DEV?: boolean } }).env?.DEV) {
      console.warn("useToast called without ToastProvider — toasts are no-ops until provider mounts.");
    }
    return {
      toast: ({ title, message, tone }: { title: string; message?: string; tone?: ToastTone }) => {
        console.warn("[toast]", tone ?? "info", title, message ?? "");
      },
      dismiss: (_id: string) => {},
      dismissMatching: (_title: string) => {},
    };
  }
  return ctx;
}

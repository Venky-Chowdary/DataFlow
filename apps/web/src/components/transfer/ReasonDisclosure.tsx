import { useId, useLayoutEffect, useRef, useState } from "react";

interface ReasonDisclosureProps {
  /** Id of the clamped wrapper — the toggle's aria-controls target. Defaults to useId(). */
  id?: string;
  /** Destination column name, used in the toggle's accessible label. */
  label: string;
  /** Full reason text; >90 chars forces the toggle even with no layout (SSR). */
  fullText: string;
  /** Start expanded (server-rendered tests and print). */
  defaultOpen?: boolean;
  children?: React.ReactNode;
}

const LONG_TEXT = 90;

/**
 * Keyboard/screen-reader access to a clamped Why cell: the full text stays in
 * the DOM and a More/Less toggle removes the clamp. A title attribute alone is
 * not reachable by keyboard or assistive technology (WCAG 2.1.1 / 1.4.13).
 * The toggle is shown when the clamped element (the first [data-clamp]
 * descendant, else the wrapper) is measurably clipped, or when the text is
 * plainly long — and it stays visible once opened so the cell can collapse.
 */
export function ReasonDisclosure({ id, label, fullText, defaultOpen = false, children }: ReasonDisclosureProps) {
  const autoId = useId();
  const wrapperId = id ?? autoId;
  const [open, setOpen] = useState(defaultOpen);
  const [clipped, setClipped] = useState(false);
  const wrapperRef = useRef<HTMLDivElement | null>(null);

  useLayoutEffect(() => {
    const wrapper = wrapperRef.current;
    if (!wrapper) return;
    const measure = () => {
      const el = (wrapper.querySelector("[data-clamp]") ?? wrapper) as HTMLElement;
      setClipped(el.scrollHeight > el.clientHeight + 1);
    };
    measure();
    if (typeof ResizeObserver !== "undefined") {
      const observer = new ResizeObserver(measure);
      observer.observe(wrapper);
      return () => observer.disconnect();
    }
  }, []);

  const showToggle = open || clipped || fullText.length > LONG_TEXT;

  return (
    <>
      <div
        id={wrapperId}
        ref={wrapperRef}
        className={`df2-column-reason-text${open ? " is-open" : ""}`}
      >
        {children}
      </div>
      {showToggle ? (
        <button
          type="button"
          className="df2-link-btn df2-column-reason-toggle"
          aria-expanded={open}
          aria-controls={wrapperId}
          aria-label={`${open ? "Hide" : "Show"} full reason for ${label}`}
          onClick={() => setOpen((v) => !v)}
        >
          {open ? "Less" : "More"}
        </button>
      ) : null}
    </>
  );
}

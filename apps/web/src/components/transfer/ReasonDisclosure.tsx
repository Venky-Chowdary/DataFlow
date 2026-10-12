import { useState } from "react";

interface ReasonDisclosureProps {
  /** Id of the clamped wrapper — the toggle's aria-controls target. */
  id: string;
  /** Destination column name, used in the toggle's accessible label. */
  label: string;
  /** Full reason text; the toggle only renders when it is longer than 90 chars. */
  fullText: string;
  /** Start expanded (server-rendered tests and print). */
  defaultOpen?: boolean;
  children?: React.ReactNode;
}

/**
 * Keyboard/screen-reader access to a clamped Why cell: the full text stays in
 * the DOM and a More/Less toggle removes the clamp. A title attribute alone is
 * not reachable by keyboard or assistive technology (WCAG 2.1.1 / 1.4.13).
 */
export function ReasonDisclosure({ id, label, fullText, defaultOpen = false, children }: ReasonDisclosureProps) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <>
      <div
        id={id}
        className={`df2-column-reason-text${open ? " is-open" : ""}`}
      >
        {children}
      </div>
      {fullText.length > 90 ? (
        <button
          type="button"
          className="df2-link-btn df2-column-reason-toggle"
          aria-expanded={open}
          aria-controls={id}
          aria-label={`${open ? "Hide" : "Show"} full reason for ${label}`}
          onClick={() => setOpen((v) => !v)}
        >
          {open ? "Less" : "More"}
        </button>
      ) : null}
    </>
  );
}

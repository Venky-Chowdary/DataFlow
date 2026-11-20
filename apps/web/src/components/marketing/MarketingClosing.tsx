import type { ReactNode } from "react";

/**
 * Closing band for a marketing page: the logo tile green, used once per page.
 * Children are the actions (one primary, one secondary).
 */
export function MarketingClosing({
  title,
  body,
  note,
  children,
}: {
  title: string;
  body: ReactNode;
  note?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="mk-closing" aria-label={title}>
      <div className="mk-wrap mk-closing-grid">
        <div>
          <h2>{title}</h2>
          <p>{body}</p>
        </div>
        <div className="mk-closing-actions">
          <div className="mk-actions">{children}</div>
          {note ? <p className="mk-caption">{note}</p> : null}
        </div>
      </div>
    </section>
  );
}

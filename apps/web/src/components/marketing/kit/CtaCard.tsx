import type { ReactNode } from "react";

/**
 * The closing section: a contained card on the logo tile ink, radius 24,
 * with the page's one gradient (a soft brand glow in a corner). A centred
 * h2, one line, the actions as pills (primary inverted to light, secondary
 * ghost) and an optional note.
 */
export function CtaCard({
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
    <section className="mk-cta" aria-label={title}>
      <div className="mk-wrap">
        <div className="mk-cta-card">
          <h2>{title}</h2>
          <p className="mk-cta-body">{body}</p>
          <div className="mk-actions mk-cta-actions">{children}</div>
          {note ? <p className="mk-cta-note">{note}</p> : null}
        </div>
      </div>
    </section>
  );
}

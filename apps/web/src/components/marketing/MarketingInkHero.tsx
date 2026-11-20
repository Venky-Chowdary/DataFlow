import type { ReactNode } from "react";

/**
 * Hero for the sales, trust, and legal pages. `doc` is the quieter document
 * header used by Privacy, Terms, DPA, and Subprocessors.
 */
export function MarketingInkHero({
  kicker,
  title,
  lead,
  slas,
  actions,
  aside,
  tone = "ink",
  meta,
}: {
  kicker: string;
  title: ReactNode;
  lead: ReactNode;
  slas?: { value: string; label: string }[];
  actions?: ReactNode;
  aside?: ReactNode;
  /** ink = product page hero. doc = document header (Privacy / Terms). */
  tone?: "ink" | "doc";
  meta?: ReactNode;
}) {
  return (
    <section className={`mk-subhero${tone === "doc" ? " mk-subhero--doc" : ""}`} aria-label={kicker}>
      <div className={`mk-wrap mk-subhero-grid${aside ? "" : " is-solo"}`}>
        <div className="mk-subhero-copy">
          <p className="mk-subhero-kicker">{kicker}</p>
          <h1>{title}</h1>
          {meta ? <p className="mk-subhero-meta">{meta}</p> : null}
          <p className="mk-subhero-lead">{lead}</p>
          {actions}
          {slas?.length ? (
            <dl className="mk-subhero-facts">
              {slas.map((s) => (
                <div key={s.label}>
                  <dt>{s.label}</dt>
                  <dd>{s.value}</dd>
                </div>
              ))}
            </dl>
          ) : null}
        </div>
        {aside ? <div className="mk-subhero-visual">{aside}</div> : null}
      </div>
    </section>
  );
}

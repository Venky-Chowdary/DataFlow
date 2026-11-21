import type { ReactNode } from "react";

interface MarketingHeroBandProps {
  /** Where the page sits, e.g. "Product, Transfer Studio". Plain text, not a badge. */
  kicker: string;
  title: string;
  lead: string;
  actions?: ReactNode;
  visual?: ReactNode;
  /** Optional trail above the title (e.g. Home › Knowledge hub). */
  breadcrumb?: ReactNode;
  /** Kept so existing call sites type-check; every subpage hero is paper now. */
  tone?: "light" | "ink";
  motion?: "default" | "pricing" | "contact" | "customers";
}

/** Subpage hero: title and lead on the page surface, one real visual beside it. */
export function MarketingHeroBand({ kicker, title, lead, actions, visual, breadcrumb }: MarketingHeroBandProps) {
  return (
    <section className="mk-subhero" aria-label={kicker}>
      <div className={`mk-wrap mk-subhero-grid${visual ? "" : " is-solo"}`}>
        <div className="mk-subhero-copy">
          {breadcrumb ? <p className="mk-subhero-crumb">{breadcrumb}</p> : null}
          <p className="mk-subhero-kicker">{kicker}</p>
          <h1>{title}</h1>
          <p className="mk-subhero-lead">{lead}</p>
          {actions}
        </div>
        {visual ? <div className="mk-subhero-visual">{visual}</div> : null}
      </div>
    </section>
  );
}

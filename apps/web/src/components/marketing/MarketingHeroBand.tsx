import type { ReactNode } from "react";

interface MarketingHeroBandProps {
  kicker: string;
  title: string;
  lead: string;
  actions?: ReactNode;
  visual?: ReactNode;
}

/** Full-width hero used on every marketing subpage — Airbyte / Devin pattern. */
export function MarketingHeroBand({ kicker, title, lead, actions, visual }: MarketingHeroBandProps) {
  return (
    <section className="lp-mkt-hero-band">
      <div className={`lp-mkt-hero-grid ${visual ? "" : "lp-mkt-hero-grid--solo"}`.trim()}>
        <div className="lp-mkt-hero-copy">
          <p className="lp-mkt-kicker">{kicker}</p>
          <h1>{title}</h1>
          <p className="lp-mkt-lead">{lead}</p>
          {actions}
        </div>
        {visual ? <div className="lp-mkt-hero-visual">{visual}</div> : null}
      </div>
    </section>
  );
}

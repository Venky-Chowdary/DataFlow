import type { ReactNode } from "react";

interface MarketingHeroBandProps {
  kicker: string;
  title: string;
  lead: string;
  actions?: ReactNode;
  visual?: ReactNode;
  /** Ink = dark enterprise hero with mesh (docs/help). Light = default wash. */
  tone?: "light" | "ink";
  /** Optional trail above the kicker (e.g. Home › Knowledge hub). */
  breadcrumb?: ReactNode;
}

/** Full-width hero used on every marketing subpage. */
export function MarketingHeroBand({
  kicker,
  title,
  lead,
  actions,
  visual,
  tone = "light",
  breadcrumb,
}: MarketingHeroBandProps) {
  return (
    <section className={`lp-mkt-hero-band ${tone === "ink" ? "lp-mkt-hero-band--ink" : ""}`.trim()}>
      <div className="lp-mkt-hero-band-glow" aria-hidden />
      <div className="lp-mkt-hero-band-mesh" aria-hidden />
      <div className={`lp-mkt-hero-grid ${visual ? "" : "lp-mkt-hero-grid--solo"}`.trim()}>
        <div className="lp-mkt-hero-copy">
          {breadcrumb ? <p className="lp-mkt-breadcrumb">{breadcrumb}</p> : null}
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

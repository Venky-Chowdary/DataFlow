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
  /**
   * Page-specific motion language so Pricing / Contact / Customers
   * do not all share the same float + aurora feel.
   */
  motion?: "default" | "pricing" | "contact" | "customers";
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
  motion = "default",
}: MarketingHeroBandProps) {
  return (
    <section
      className={[
        "lp-mkt-hero-band",
        tone === "ink" ? "lp-mkt-hero-band--ink" : "",
        motion !== "default" ? `lp-mkt-hero-band--${motion}` : "",
      ].filter(Boolean).join(" ")}
    >
      <div className="lp-mkt-hero-band-glow" aria-hidden />
      <div className="lp-mkt-hero-band-mesh" aria-hidden />
      {motion === "contact" ? <div className="lp-mkt-hero-band-beams" aria-hidden /> : null}
      {motion === "pricing" ? <div className="lp-mkt-hero-band-ruler" aria-hidden /> : null}
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

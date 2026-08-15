import type { ReactNode } from "react";

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
  /** ink = dark product hero. doc = light document header (Privacy / Terms). */
  tone?: "ink" | "doc";
  meta?: ReactNode;
}) {
  const slaList = slas?.length ? (
    <ul className="lp-sales-hero-slas">
      {slas.map((s) => (
        <li key={s.label}>
          <strong>{s.value}</strong>
          <span>{s.label}</span>
        </li>
      ))}
    </ul>
  ) : null;

  return (
    <section
      className={[
        "lp-sales-hero",
        tone === "doc" ? "lp-sales-hero--doc" : "lp-sales-hero--ink",
        aside ? "lp-sales-hero--split" : "lp-sales-hero--page",
      ].join(" ")}
      aria-label={kicker}
    >
      <div className="lp-mkt-wrap lp-sales-hero-inner">
        <div className="lp-sales-hero-copy">
          <p className="lp-sales-kicker">{kicker}</p>
          <h1>{title}</h1>
          {meta ? <p className="lp-sales-hero-meta">{meta}</p> : null}
          <p className="lp-sales-hero-lead">{lead}</p>
          {actions}
          {slaList}
        </div>
        {aside}
      </div>
    </section>
  );
}

import type { ReactNode } from "react";

export function MarketingInkHero({
  kicker,
  title,
  lead,
  slas,
  actions,
  aside,
}: {
  kicker: string;
  title: ReactNode;
  lead: ReactNode;
  slas?: { value: string; label: string }[];
  actions?: ReactNode;
  aside?: ReactNode;
}) {
  const slaList = slas?.length ? (
    <ul className={`lp-sales-hero-slas${aside ? "" : " lp-sales-hero-slas--panel"}`}>
      {slas.map((s) => (
        <li key={s.label}>
          <strong>{s.value}</strong>
          <span>{s.label}</span>
        </li>
      ))}
    </ul>
  ) : null;

  return (
    <section className="lp-sales-hero lp-sales-hero--page" aria-label={kicker}>
      <div className="lp-mkt-wrap lp-sales-hero-inner">
        <div className="lp-sales-hero-copy">
          <p className="lp-sales-kicker">{kicker}</p>
          <h1>{title}</h1>
          <p>{lead}</p>
          {actions}
          {aside ? slaList : null}
        </div>
        {aside ?? slaList}
      </div>
    </section>
  );
}

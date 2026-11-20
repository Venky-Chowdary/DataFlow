import type { MouseEvent, ReactNode } from "react";
import { ArrowRight, type LucideIcon } from "lucide-react";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { hashForPublicRoute, type PublicRoute } from "../../lib/publicNavigation";

export type StoryAction = { label: string; onClick: () => void };

export type Related = { route: PublicRoute; label: string; body: string };

/**
 * The one structure every product and solution page follows (pass 5):
 * a left-aligned hero with one light product frame under it, three to five
 * body sections (feature rows that alternate sides, a grid of facts, or a
 * table), related links, and the closing section on the page background.
 */
export function StoryFrame({
  page,
  hero,
  children,
  related,
  closing,
  actions,
  onNavigate,
}: {
  /** Page prefix, e.g. "ts" for Transfer Studio. Becomes `mk-sb mk-<page>`. */
  page: string;
  hero: ReactNode;
  children: ReactNode;
  related: Related[];
  closing: { title: string; body: string };
  actions: { primary: StoryAction; secondary: StoryAction };
  onNavigate: (r: PublicRoute) => void;
}) {
  const go = (route: PublicRoute) => (e: MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault();
    onNavigate(route);
  };
  return (
    <div className={`mk-page mk-sb mk-${page}`}>
      {hero}
      {children}
      <nav className="mk-sb-related" aria-labelledby={`mk-${page}-related`}>
        <div className="mk-wrap">
          <p id={`mk-${page}-related`} className="mk-sb-related-title">
            Related
          </p>
          <ul>
            {related.map((r) => (
              <li key={r.route}>
                <a href={hashForPublicRoute(r.route)} onClick={go(r.route)}>
                  <strong>
                    {r.label}
                    <ArrowRight size={16} aria-hidden />
                  </strong>
                  <span>{r.body}</span>
                </a>
              </li>
            ))}
          </ul>
        </div>
      </nav>
      <MarketingClosing title={closing.title} body={closing.body}>
        <StoryActions {...actions} />
      </MarketingClosing>
    </div>
  );
}

export function StoryActions({ primary, secondary }: { primary: StoryAction; secondary: StoryAction }) {
  return (
    <>
      <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={primary.onClick}>
        {primary.label}
      </button>
      <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={secondary.onClick}>
        {secondary.label}
      </button>
    </>
  );
}

/**
 * The hero: the h1, one lead and two actions, left-aligned; then one light
 * product frame at full content width. Scenes crop to a single panel on
 * phones, so nothing in the frame is cut off.
 */
export function StoryHero({
  id,
  title,
  lead,
  actions,
  children,
}: {
  id: string;
  title: ReactNode;
  lead: ReactNode;
  actions: { primary: StoryAction; secondary: StoryAction };
  children: ReactNode;
}) {
  return (
    <section className="mk-sb-hero" aria-labelledby={id}>
      <div className="mk-wrap">
        <div className="mk-sb-copy">
          <h1 id={id}>{title}</h1>
          <p className="mk-sb-lead">{lead}</p>
          <div className="mk-sb-actions">
            <StoryActions {...actions} />
          </div>
        </div>
        <div className="mk-sb-hero-frame">{children}</div>
      </div>
    </section>
  );
}

/** A heading and, at most, one sentence under it. */
export function SectionHead({ id, title, body, className = "" }: { id: string; title: ReactNode; body?: ReactNode; className?: string }) {
  return (
    <header className={`mk-sb-head ${className}`.trim()}>
      <h2 id={id}>{title}</h2>
      {body ? <p>{body}</p> : null}
    </header>
  );
}

/**
 * A feature row: short text on one side, one light product frame on the
 * other. Rows alternate sides down the page (`flip` puts the frame first).
 */
export function FeatureRow({
  id,
  title,
  body,
  flip = false,
  more,
  className = "",
  children,
}: {
  id: string;
  title: ReactNode;
  body: ReactNode;
  flip?: boolean;
  /** A short line or list under the lead, kept small. */
  more?: ReactNode;
  className?: string;
  children: ReactNode;
}) {
  return (
    <section className={`mk-sb-section mk-sb-row${flip ? " is-flip" : ""} ${className}`.trim()} aria-labelledby={id}>
      <div className="mk-wrap mk-sb-row-grid">
        <div className="mk-sb-row-copy">
          <SectionHead id={id} title={title} body={body} />
          {more}
        </div>
        <div className="mk-sb-row-frame">{children}</div>
      </div>
    </section>
  );
}

export type RowPoint = { icon: LucideIcon; lead?: string; text: string };

/**
 * Two or three short points under a feature row's lead: a line icon and one
 * line each, with an optional bold lead-in that starts the sentence. Each
 * point restates something the product docs already say. An optional link
 * goes to the in-site docs article for the row.
 */
export function RowPoints({
  items,
  link,
}: {
  items: RowPoint[];
  link?: { label: string; route: PublicRoute; onNavigate: (r: PublicRoute) => void };
}) {
  return (
    <div className="mk-sb-points">
      <ul>
        {items.map(({ icon: Icon, lead, text }) => (
          <li key={text}>
            <Icon size={18} strokeWidth={1.75} aria-hidden />
            <span>
              {lead ? <strong>{lead}</strong> : null}
              {lead ? " " : null}
              {text}
            </span>
          </li>
        ))}
      </ul>
      {link ? (
        <p className="mk-sb-points-link">
          <a
            href={hashForPublicRoute(link.route)}
            onClick={(e) => {
              e.preventDefault();
              link.onNavigate(link.route);
            }}
          >
            {link.label}
            <ArrowRight size={16} aria-hidden />
          </a>
        </p>
      ) : null}
    </div>
  );
}

export type Fact = { icon: LucideIcon; title: string; body: ReactNode };

/** Three columns of short facts: a line icon, a title and two lines. */
export function FactGrid({ id, title, body, items, className = "" }: { id: string; title: ReactNode; body?: ReactNode; items: Fact[]; className?: string }) {
  return (
    <section className={`mk-sb-section mk-sb-facts ${className}`.trim()} aria-labelledby={id}>
      <div className="mk-wrap">
        <SectionHead id={id} title={title} body={body} />
        <ul className="mk-sb-facts-grid">
          {items.map(({ icon: Icon, title: t, body: b }) => (
            <li key={t}>
              <Icon size={20} strokeWidth={1.75} aria-hidden />
              <h3>{t}</h3>
              <p>{b}</p>
            </li>
          ))}
        </ul>
      </div>
    </section>
  );
}

/** A section whose body is a table. */
export function TableSection({ id, title, body, className = "", children }: { id: string; title: ReactNode; body?: ReactNode; className?: string; children: ReactNode }) {
  return (
    <section className={`mk-sb-section mk-sb-tablesec ${className}`.trim()} aria-labelledby={id}>
      <div className="mk-wrap">
        <SectionHead id={id} title={title} body={body} />
        {children}
      </div>
    </section>
  );
}

/**
 * Rules a reader can hold the product to, and where each one is written
 * down. A real two-column table, ruled like one and unboxed.
 */
export function RulesTable({
  caption,
  items,
  ruleLabel = "Rule",
  whereLabel = "Where to check",
}: {
  caption: string;
  items: { claim: string; where: string }[];
  ruleLabel?: string;
  whereLabel?: string;
}) {
  return (
    <table className="mk-sb-table mk-sb-rules">
      <caption className="mk-sr-only">{caption}</caption>
      <thead>
        <tr>
          <th scope="col">{ruleLabel}</th>
          <th scope="col">{whereLabel}</th>
        </tr>
      </thead>
      <tbody>
        {items.map((c) => (
          <tr key={c.claim}>
            <th scope="row">{c.claim}</th>
            <td>{c.where}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

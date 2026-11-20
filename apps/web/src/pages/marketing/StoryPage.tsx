import type { MouseEvent, ReactNode } from "react";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { ProductShot } from "../../components/marketing/ProductShot";
import { hashForPublicRoute, type PublicRoute } from "../../lib/publicNavigation";

export type StoryAction = { label: string; onClick: () => void };

export type Capture = { src: string; alt: string; caption: string };

export type Related = { route: PublicRoute; label: string; body: string };

/**
 * The kit every product and solution page is built from. The frame is only
 * the chrome around a page (related links and the closing band); each page
 * composes its own hero and body from the parts below, so no two pages share
 * a layout.
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
      <nav className="mk-sb-related" aria-label="Related pages">
        <div className="mk-wrap">
          <ul>
            {related.map((r) => (
              <li key={r.route}>
                <a href={hashForPublicRoute(r.route)} onClick={go(r.route)}>
                  <strong>{r.label}</strong>
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

/** Title, one sentence, and the two actions. Layout belongs to the page. */
export function HeroCopy({
  id,
  title,
  lead,
  actions,
  className = "",
}: {
  id: string;
  title: ReactNode;
  lead: ReactNode;
  actions: { primary: StoryAction; secondary: StoryAction };
  className?: string;
}) {
  return (
    <div className={`mk-sb-copy ${className}`.trim()}>
      <h1 id={id}>{title}</h1>
      <p className="mk-sb-lead">{lead}</p>
      <div className="mk-sb-actions">
        <StoryActions {...actions} />
      </div>
    </div>
  );
}

/** Product UI sits on the dark screen: this wrapper carries its variables. */
export function Screen({ className = "", children, label }: { className?: string; children: ReactNode; label?: string }) {
  return (
    <div className={`mk-sb-screen ${className}`.trim()} role={label ? "group" : undefined} aria-label={label}>
      {children}
    </div>
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
 * A full product screen (an AppWindow scene) on its own stage, with one
 * line of action under it. The scene's caption says the data is an example.
 */
export function FrameSection({
  id,
  title,
  body,
  tone,
  cta,
  notes,
  className = "",
  children,
}: {
  id: string;
  title: ReactNode;
  body?: ReactNode;
  tone: "dark" | "band" | "paper";
  cta: StoryAction;
  /** What the scene's numbered pins point at, in order. */
  notes?: readonly string[];
  className?: string;
  children: ReactNode;
}) {
  return (
    <section className={`mk-sb-frame is-${tone}${tone === "dark" ? " mk-sb-dark" : ""} ${className}`.trim()} aria-labelledby={id}>
      <div className="mk-wrap">
        <SectionHead id={id} title={title} body={body} />
        {children}
        {notes?.length ? (
          <ol className="mk-sb-notes" aria-label="What the numbers in the frame point at">
            {notes.map((n) => (
              <li key={n}>{n}</li>
            ))}
          </ol>
        ) : null}
        <p className="mk-sb-frame-cta">
          <button type="button" className="mk-btn mk-btn--secondary" onClick={cta.onClick}>
            {cta.label}
          </button>
        </p>
      </div>
    </section>
  );
}

/** A crop of the live workspace, mounted on a sheet. */
export function StoryCapture({ capture, className = "" }: { capture: Capture; className?: string }) {
  return (
    <div className={`mk-sb-capture ${className}`.trim()}>
      <ProductShot {...capture} />
    </div>
  );
}

/**
 * Rules a reader can hold the product to, and where each one is written
 * down. This is a real two-column table, so it is ruled like one.
 */
export function RulesLedger({
  id,
  title,
  items,
  whereLabel = "Where to check",
  className = "",
}: {
  id: string;
  title: string;
  items: { claim: string; where: string }[];
  whereLabel?: string;
  className?: string;
}) {
  return (
    <div className={`mk-sb-rules ${className}`.trim()}>
      <h3 id={id}>{title}</h3>
      <table aria-labelledby={id}>
        <thead>
          <tr>
            <th scope="col">Rule</th>
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
    </div>
  );
}

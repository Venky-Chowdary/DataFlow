import type { MouseEvent, ReactNode } from "react";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { ProcessStepper, type ProcessStep } from "../../components/marketing/ProcessStepper";
import { ProductShot } from "../../components/marketing/ProductShot";
import { hashForPublicRoute, type PublicRoute } from "../../lib/publicNavigation";

export type StoryAction = { label: string; onClick: () => void };

export type Story = {
  /** Which of the five steps this page is about. */
  steps: ProcessStep[];
  kicker: string;
  title: string;
  lead: string;
  /** "Start free" on every product and solution page. */
  primary: StoryAction;
  /** One page-specific secondary, repeated in the closing band. */
  secondary: StoryAction;
  hero: ReactNode;
  problem: { title: string; body: string; failures: { title: string; body: string }[] };
  how: { title: string; lead?: string; sequence?: boolean; points: { title: string; body: string }[] };
  surface: {
    title: string;
    body: string;
    visual?: ReactNode;
    capture?: { src: string; alt: string; surface: string; route?: { source: string; dest: string } };
  };
  checks: { title: string; lead?: string; items: { claim: string; where: string }[] };
  related: { route: PublicRoute; label: string; body: string }[];
  closing: { title: string; body: string };
};

/**
 * One story per product or solution page, kept to three sections after the
 * hero: the problem in the buyer's terms, how Datawrap handles it, and where
 * you see it in the product together with what you can check yourself.
 */
export function StoryPage({ story, onNavigate }: { story: Story; onNavigate: (r: PublicRoute) => void }) {
  const go = (route: PublicRoute) => (e: MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault();
    onNavigate(route);
  };
  const actions = (
    <>
      <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={story.primary.onClick}>
        {story.primary.label}
      </button>
      <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={story.secondary.onClick}>
        {story.secondary.label}
      </button>
    </>
  );
  return (
    <div className="mk-page mk-story">
      <MarketingHeroBand
        kicker={story.kicker}
        title={story.title}
        lead={story.lead}
        actions={<div className="lp-hero-cta">{actions}</div>}
        visual={story.hero}
      />

      <section className="mk-section mk-story-problem" aria-labelledby="mk-story-problem">
        <div className="mk-wrap mk-story-grid">
          <header className="mk-story-head">
            <h2 id="mk-story-problem">{story.problem.title}</h2>
          </header>
          <div className="mk-story-body">
            <p className="mk-story-lede">{story.problem.body}</p>
            <dl className="mk-story-failures">
              {story.problem.failures.map((f) => (
                <div key={f.title}>
                  <dt>{f.title}</dt>
                  <dd>{f.body}</dd>
                </div>
              ))}
            </dl>
          </div>
        </div>
      </section>

      <section className="mk-section mk-section--band mk-story-how" aria-labelledby="mk-story-how">
        <div className="mk-wrap">
          <header className="mk-section-head mk-story-how-head">
            <div>
              <h2 id="mk-story-how">{story.how.title}</h2>
              {story.how.lead ? <p>{story.how.lead}</p> : null}
            </div>
            <ProcessStepper current={story.steps} />
          </header>
          {story.how.sequence ? (
            <ol className="mk-story-points is-sequence">
              {story.how.points.map((p, i) => (
                <li key={p.title}>
                  <span className="mk-step-num" aria-hidden>
                    {i + 1}
                  </span>
                  <h3>{p.title}</h3>
                  <p>{p.body}</p>
                </li>
              ))}
            </ol>
          ) : (
            <ul className="mk-story-points">
              {story.how.points.map((p) => (
                <li key={p.title}>
                  <h3>{p.title}</h3>
                  <p>{p.body}</p>
                </li>
              ))}
            </ul>
          )}
        </div>
      </section>

      <section className="mk-section mk-story-surface" aria-labelledby="mk-story-surface">
        <div className="mk-wrap">
          <div className="mk-split">
            <header className="mk-section-head mk-section-head--stack">
              <h2 id="mk-story-surface">{story.surface.title}</h2>
              <p>{story.surface.body}</p>
            </header>
            {story.surface.visual ? <div className="mk-split-visual">{story.surface.visual}</div> : null}
          </div>
          {story.surface.capture ? (
            <figure className="mk-shots">
              <ProductShot
                src={story.surface.capture.src}
                alt={story.surface.capture.alt}
                surface={story.surface.capture.surface}
                route={story.surface.capture.route}
              />
              <figcaption className="mk-caption">The same screen in the product, as it ships.</figcaption>
            </figure>
          ) : null}
          <div className="mk-story-checks">
            <h3>{story.checks.title}</h3>
            {story.checks.lead ? <p>{story.checks.lead}</p> : null}
            <dl className="mk-checks">
              {story.checks.items.map((c) => (
                <div key={c.claim}>
                  <dt>{c.claim}</dt>
                  <dd>{c.where}</dd>
                </div>
              ))}
            </dl>
          </div>
        </div>
      </section>

      <nav className="mk-related" aria-label="Related pages">
        <div className="mk-wrap">
          <ul>
            {story.related.map((r) => (
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

      <MarketingClosing title={story.closing.title} body={story.closing.body}>
        {actions}
      </MarketingClosing>
    </div>
  );
}

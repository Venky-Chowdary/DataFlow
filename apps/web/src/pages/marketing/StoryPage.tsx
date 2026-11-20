import type { MouseEvent, ReactNode } from "react";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { ProcessStepper, type ProcessStep } from "../../components/marketing/ProcessStepper";
import { ProductShot } from "../../components/marketing/ProductShot";
import { hashForPublicRoute, type PublicRoute } from "../../lib/publicNavigation";

export type StoryAction = { label: string; onClick: () => void };

export type Capture = { src: string; alt: string; caption: string };

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
  /** A statement page has no failure list; a split page names one or two. */
  problem: { title: string; body: string; failures?: { title: string; body: string }[] };
  /** Points are not numbered: the stepper is the page's one sequence. */
  how?: { title: string; lead?: string; points: { title: string; body: string; step?: ProcessStep }[] };
  /** The module only this page has, and where it sits. */
  signature?: { at: "afterProblem" | "afterHow"; title: string; body?: string; node: ReactNode; band?: boolean };
  surface: { title: string; body: string; visual?: ReactNode; capture?: Capture };
  checks: { title: string; lead?: string; items: { claim: string; where: string }[]; capture?: Capture };
  related: { route: PublicRoute; label: string; body: string }[];
  closing: { title: string; body: string };
};

/**
 * One story per product or solution page. The frame is shared (hero,
 * problem, the page's own module, where you see it, related, close); the
 * rhythm is not: problem sections are a statement or a split, and each page
 * places its own module where its argument needs it.
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
  const failures = story.problem.failures ?? [];
  const signature = story.signature ? (
    <section
      className={`mk-section mk-story-signature${story.signature.band ? " mk-section--band" : ""}`}
      aria-labelledby="mk-story-signature"
    >
      <div className="mk-wrap">
        <header className="mk-section-head">
          <h2 id="mk-story-signature">{story.signature.title}</h2>
          {story.signature.body ? <p>{story.signature.body}</p> : null}
        </header>
        {story.signature.node}
      </div>
    </section>
  ) : null;

  return (
    <div className="mk-page mk-story">
      <MarketingHeroBand
        kicker={story.kicker}
        title={story.title}
        lead={story.lead}
        actions={<div className="lp-hero-cta">{actions}</div>}
        visual={story.hero}
      />

      {failures.length ? (
        <section className="mk-section mk-story-problem" aria-labelledby="mk-story-problem">
          <div className="mk-wrap mk-story-grid">
            <header className="mk-story-head">
              <h2 id="mk-story-problem">{story.problem.title}</h2>
            </header>
            <div className="mk-story-body">
              <p className="mk-story-lede">{story.problem.body}</p>
              <dl className="mk-story-failures">
                {failures.map((f) => (
                  <div key={f.title}>
                    <dt>{f.title}</dt>
                    <dd>{f.body}</dd>
                  </div>
                ))}
              </dl>
            </div>
          </div>
        </section>
      ) : (
        <section className="mk-section mk-story-statement" aria-labelledby="mk-story-problem">
          <div className="mk-wrap">
            <h2 id="mk-story-problem">{story.problem.title}</h2>
            <p>{story.problem.body}</p>
          </div>
        </section>
      )}

      {story.signature?.at === "afterProblem" ? signature : null}

      {story.how ? (
        <section className="mk-section mk-section--band mk-story-how" aria-labelledby="mk-story-how">
          <div className="mk-wrap">
            <header className="mk-section-head mk-story-how-head">
              <div>
                <h2 id="mk-story-how">{story.how.title}</h2>
                {story.how.lead ? <p>{story.how.lead}</p> : null}
              </div>
              <ProcessStepper current={story.steps} />
            </header>
            <ul className="mk-story-points">
              {story.how.points.map((p) => (
                <li key={p.title}>
                  {p.step ? <p className="mk-story-step">{p.step}</p> : null}
                  <h3>{p.title}</h3>
                  <p>{p.body}</p>
                </li>
              ))}
            </ul>
          </div>
        </section>
      ) : null}

      {story.signature?.at === "afterHow" ? signature : null}

      <section className="mk-section mk-story-surface" aria-labelledby="mk-story-surface">
        <div className="mk-wrap">
          <div className={`mk-split${story.surface.visual || story.surface.capture ? "" : " is-solo"}`}>
            <header className="mk-section-head mk-section-head--stack">
              <h2 id="mk-story-surface">{story.surface.title}</h2>
              <p>{story.surface.body}</p>
              {!story.how ? <ProcessStepper current={story.steps} /> : null}
            </header>
            {story.surface.visual || story.surface.capture ? (
              <div className="mk-split-visual">
                {story.surface.visual}
                {story.surface.capture ? <ProductShot {...story.surface.capture} /> : null}
              </div>
            ) : null}
          </div>
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
            {story.checks.capture ? <ProductShot className="mk-checks-shot" {...story.checks.capture} /> : null}
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

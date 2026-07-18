import type { ReactNode } from "react";

interface MarketingFigureProps {
  /** Chrome-bar label — e.g. the app surface a screenshot would show. */
  label: string;
  caption: string;
  children: ReactNode;
  /** Optional step number for step-by-step walkthroughs. */
  step?: number;
}

/**
 * Documentation figure — frames an illustration or app screenshot inside a
 * browser-style chrome bar with a caption, the way polished help articles
 * present step-by-step visuals. Content is our own SVG/diagram or screenshot.
 */
export function MarketingFigure({ label, caption, children, step }: MarketingFigureProps) {
  return (
    <figure className="lp-mkt-figure">
      <div className="lp-mkt-figure-frame">
        <div className="lp-mkt-figure-bar">
          <span className="lp-mkt-figure-dot" />
          <span className="lp-mkt-figure-dot" />
          <span className="lp-mkt-figure-dot" />
          <span className="lp-mkt-figure-bar-label">{label}</span>
        </div>
        <div className="lp-mkt-figure-body">{children}</div>
      </div>
      <figcaption className="lp-mkt-figure-caption">
        {step != null && <span className="lp-mkt-figure-step">Step {step}</span>}
        {caption}
      </figcaption>
    </figure>
  );
}

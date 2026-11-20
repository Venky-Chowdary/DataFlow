import type { ReactNode } from "react";

/**
 * A 12-column grid of tiles. Tiles differ in width because their content
 * differs in weight; below 1024px every tile takes the full row.
 */
export function Bento({ children, className = "", label }: { children: ReactNode; className?: string; label?: string }) {
  return (
    <div className={`mk-kit-bento ${className}`.trim()} role={label ? "group" : undefined} aria-label={label}>
      {children}
    </div>
  );
}

export type TileSpan = 3 | 4 | 5 | 6 | 7 | 8 | 9 | 12;

/**
 * One tile: an h3 that names what the user gets, one plain sentence, and a
 * fragment of the real product UI at the foot (`.mk-app-*` classes). The UI
 * may be cropped at the tile edge (`bleed`), never shrunk.
 *
 * `step` marks a tile that is one step of a real sequence (Plan, Validate,
 * Execute, Verify); `figure` puts the tile's figure beside its title; `op`
 * draws the operator that joins this tile to the one before it when a row
 * of tiles is an equation.
 */
export function BentoTile({
  span = 6,
  title,
  body,
  step,
  figure,
  op,
  bleed = false,
  tone,
  className = "",
  children,
}: {
  span?: TileSpan;
  title: ReactNode;
  body?: ReactNode;
  step?: { n: number; label: string };
  figure?: ReactNode;
  op?: "+" | "=" | "−";
  bleed?: boolean;
  tone?: "landed" | "held" | "skipped" | "balance";
  className?: string;
  children?: ReactNode;
}) {
  const cls = ["mk-tile", `mk-tile--${span}`, tone ? `mk-tile--${tone}` : "", className].filter(Boolean).join(" ");
  return (
    <article className={cls}>
      {op ? (
        <span className="mk-tile-op" aria-hidden>
          {op}
        </span>
      ) : null}
      <div className="mk-tile-head">
        {step ? (
          <p className="mk-tile-step">
            <span className="mk-tile-step-n" aria-hidden>
              {step.n}
            </span>
            <span>
              <span className="mk-sr-only">Step {step.n}: </span>
              {step.label}
            </span>
          </p>
        ) : null}
        <div className="mk-tile-title">
          <h3>{title}</h3>
          {figure !== undefined ? <p className="mk-tile-figure">{figure}</p> : null}
        </div>
        {body ? <p className="mk-tile-body">{body}</p> : null}
      </div>
      {children ? <div className={`mk-tile-ui${bleed ? " is-bleed" : ""}`}>{children}</div> : null}
    </article>
  );
}

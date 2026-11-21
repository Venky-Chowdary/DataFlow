import type { MouseEvent, ReactNode } from "react";
import { hashForPublicRoute, type PublicRoute } from "../../../lib/publicNavigation";

export type Stat = {
  /** The figure, with its relation ("21 of 46"): computed from data. */
  figure: ReactNode;
  /** The rest of the sentence the figure belongs to. */
  text: ReactNode;
  /** The in-site page that backs the figure. */
  route?: PublicRoute;
};

/**
 * Figures that belong together, set as one ruled statement: each figure
 * reads into its own sentence ("21 of 46 drivers proven in CI"), separated
 * by hairlines. Figures are Plex Sans with tabular numerals. Only for
 * figures already in the repo, computed from data where data exists.
 */
export function StatRow({
  items,
  label,
  onNavigate,
}: {
  items: Stat[];
  label: string;
  onNavigate?: (r: PublicRoute) => void;
}) {
  return (
    <ul className="mk-statrow" aria-label={label}>
      {items.map((s, i) => {
        const inner = (
          <>
            <span className="mk-statrow-fig">{s.figure}</span> <span className="mk-statrow-text">{s.text}</span>
          </>
        );
        return (
          <li key={i}>
            {s.route && onNavigate ? (
              <a
                href={hashForPublicRoute(s.route)}
                onClick={(e: MouseEvent<HTMLAnchorElement>) => {
                  e.preventDefault();
                  onNavigate(s.route as PublicRoute);
                }}
              >
                {inner}
              </a>
            ) : (
              <span>{inner}</span>
            )}
          </li>
        );
      })}
    </ul>
  );
}

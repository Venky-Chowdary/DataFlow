import type { MouseEvent } from "react";
import type { LucideIcon } from "lucide-react";
import { hashForPublicRoute, type PublicRoute } from "../../../lib/publicNavigation";

export type FeatureLink = { icon: LucideIcon; label: string; body: string; route: PublicRoute };

/**
 * Three or four small links under a large visual: a 16px line icon, an
 * underlined label and one short line. Every item is a real in-site route.
 */
export function FeatureLinks({
  items,
  label,
  onNavigate,
}: {
  items: FeatureLink[];
  label: string;
  onNavigate: (r: PublicRoute) => void;
}) {
  return (
    <nav className="mk-flinks" aria-label={label}>
      <div className="mk-wrap">
        <ul>
          {items.map(({ icon: Icon, label: l, body, route }) => (
            <li key={route}>
              <a
                href={hashForPublicRoute(route)}
                onClick={(e: MouseEvent<HTMLAnchorElement>) => {
                  e.preventDefault();
                  onNavigate(route);
                }}
              >
                <Icon size={16} strokeWidth={1.75} aria-hidden />
                <span className="mk-flinks-label">{l}</span>
                <span className="mk-flinks-body">{body}</span>
              </a>
            </li>
          ))}
        </ul>
      </div>
    </nav>
  );
}

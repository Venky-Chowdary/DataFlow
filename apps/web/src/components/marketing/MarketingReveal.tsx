import type { ReactNode } from "react";

/**
 * Section wrapper for marketing subpages. Sections used to fade and slide in
 * on scroll; the site keeps motion for the one load moment on the home page
 * and for responses to what a visitor does, so this is a plain wrapper now.
 */
export function MarketingReveal({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <div className={className || undefined}>{children}</div>;
}

import type { ReactNode } from "react";

interface MarketingSectionFooterProps {
  children: ReactNode;
  align?: "center" | "start";
}

/** Dense CTA row aligned to the marketing shell. */
export function MarketingSectionFooter({ children, align = "center" }: MarketingSectionFooterProps) {
  return (
    <div className={`lp-section-footer lp-section-footer--${align} lp-mkt-next-cta`}>
      {children}
    </div>
  );
}

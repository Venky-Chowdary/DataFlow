import type { ReactNode } from "react";

interface MarketingSectionFooterProps {
  children: ReactNode;
  align?: "center" | "start";
}

/** Centered CTA row — never a stray left-aligned text link. */
export function MarketingSectionFooter({ children, align = "center" }: MarketingSectionFooterProps) {
  return (
    <div className={`lp-section-footer lp-section-footer--${align}`}>
      {children}
    </div>
  );
}

import type { ReactNode } from "react";
import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";

export function MarketingReveal({ children, className = "" }: { children: ReactNode; className?: string }) {
  const { ref, className: revealCls } = useRevealOnScroll();
  return (
    <div ref={ref} className={`${revealCls} ${className}`.trim()}>
      {children}
    </div>
  );
}

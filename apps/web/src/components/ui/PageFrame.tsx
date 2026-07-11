import { ReactNode } from "react";
import { PlatformHonestyBanner } from "../PlatformHonestyBanner";

interface PageFrameProps {
  children: ReactNode;
  className?: string;
  /** Show transfer-ready honesty strip at top of page content */
  showHonesty?: boolean;
  compactHonesty?: boolean;
}

/** Standard page content wrapper — consistent vertical rhythm + optional honesty banner.
 *  Pattern: PageShell → PageFrame [showHonesty] → PageInsightStrip → PageMetricsRow → FilterTabs → content
 */
export function PageFrame({
  children,
  className = "",
  showHonesty = false,
  compactHonesty = true,
}: PageFrameProps) {
  return (
    <div className={`df2-page-workspace ${className}`.trim()}>
      {showHonesty && <PlatformHonestyBanner compact={compactHonesty} />}
      {children}
    </div>
  );
}

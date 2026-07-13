import { ReactNode } from "react";

interface PageFrameProps {
  children: ReactNode;
  className?: string;
}

/** Standard page content wrapper — consistent vertical rhythm. */
export function PageFrame({ children, className = "" }: PageFrameProps) {
  return <div className={`df2-page-workspace ${className}`.trim()}>{children}</div>;
}

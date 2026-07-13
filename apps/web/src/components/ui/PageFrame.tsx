import { ReactNode } from "react";

interface PageFrameProps {
  children: ReactNode;
  className?: string;
  showHonesty?: boolean;
}

/** Standard page content wrapper — consistent vertical rhythm. */
export function PageFrame({ children, className = "", showHonesty }: PageFrameProps) {
  return (
    <div className={`df2-page-workspace ${className} ${showHonesty ? "show-honesty" : ""}`.trim()}>
      {children}
    </div>
  );
}

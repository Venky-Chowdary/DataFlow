import type { ReactNode } from "react";

interface ButtonGroupProps {
  label: string;
  children: ReactNode;
  className?: string;
}

/** One row of actions. Gap comes from the token, not from a per-screen override. */
export function ButtonGroup({ label, children, className = "" }: ButtonGroupProps) {
  return (
    <div className={`df2-button-group ${className}`.trim()} role="toolbar" aria-label={label}>
      {children}
    </div>
  );
}

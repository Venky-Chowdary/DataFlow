import type { ReactNode } from "react";

/**
 * A section head in two columns: the h2 on the left (seven columns), one
 * line of lead on the right (five), top-aligned. Stacks below 1024px.
 * Used where a visual follows the head. No eyebrow label above the h2.
 */
export function SplitHead({
  id,
  title,
  lead,
  className = "",
}: {
  id: string;
  title: ReactNode;
  lead?: ReactNode;
  className?: string;
}) {
  return (
    <header className={`mk-split ${className}`.trim()}>
      <h2 id={id}>{title}</h2>
      {lead ? <p className="mk-split-lead">{lead}</p> : null}
    </header>
  );
}

import type { ReactNode } from "react";
import { CtaCard } from "./kit/CtaCard";

/**
 * Closing section for a marketing page, used once per page. Renders the
 * kit's CtaCard: a contained card on the logo tile ink. Children are the
 * actions (one primary, one secondary).
 */
export function MarketingClosing({
  title,
  body,
  note,
  children,
}: {
  title: string;
  body: ReactNode;
  note?: ReactNode;
  children: ReactNode;
}) {
  return (
    <CtaCard title={title} body={body} note={note}>
      {children}
    </CtaCard>
  );
}

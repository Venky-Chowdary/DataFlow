/**
 * Where "Open quarantine" goes on the job page (QA browser 4).
 *
 * The Theater's quarantine section has one id. Every button that opens it —
 * the trust card, the Gate-8 card — scrolls to that id, never to a shared
 * class name another panel also wears.
 */
export const THEATER_QUARANTINE_ID = "df2-theater-quarantine";

export function scrollToTheaterQuarantine(doc: Pick<Document, "getElementById"> = document): boolean {
  const el = doc.getElementById(THEATER_QUARANTINE_ID);
  if (!el) return false;
  el.scrollIntoView({ behavior: "smooth", block: "start" });
  return true;
}

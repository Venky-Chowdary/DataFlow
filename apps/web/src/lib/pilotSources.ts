/**
 * Citation labelling for Pilot answers. Pure so it is testable without a DOM:
 * a wrong href sends the operator to a route that does not exist, which reads as
 * a fabricated citation.
 */

import type { CopilotSource } from "./api";

export function pilotSourceLabel(source: CopilotSource): string {
  return source.title || [source.doc, source.section].filter(Boolean).join(" › ") || "";
}

/**
 * The API cites a section as `#/help/<slug>#<section-id>`, but the help router only
 * resolves the article hash — a second fragment matches no route, so link the article.
 */
export function pilotSourceHash(href: string | undefined): string | undefined {
  if (!href || !href.startsWith("#/help/")) return undefined;
  const article = href.split("#").filter(Boolean)[0];
  return article ? `#${article}` : undefined;
}

/** Only citations that can be labelled are shown; an unlabelled source is noise. */
export function citedSources(sources: CopilotSource[] | undefined): CopilotSource[] {
  return (sources || []).filter((s) => pilotSourceLabel(s));
}

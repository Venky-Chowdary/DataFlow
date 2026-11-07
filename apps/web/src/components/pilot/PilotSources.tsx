/**
 * Citations for a Pilot answer. An answer the operator cannot trace back to a
 * documentation section is prose, so a grounded turn must show where it came from.
 * Shared by the Pilot page and the rail so both surfaces cite identically.
 */

import type { CopilotSource } from "../../lib/api";
import { citedSources, pilotSourceHash, pilotSourceLabel } from "../../lib/pilotSources";

export function PilotSources({ sources }: { sources?: CopilotSource[] }) {
  const cited = citedSources(sources);
  if (cited.length === 0) return null;
  return (
    <div className="df2-copilot-sources">
      <span className="df2-copilot-sources-label">Sources</span>
      {cited.map((source, i) => {
        const label = pilotSourceLabel(source);
        const hash = pilotSourceHash(source.href);
        return hash ? (
          <a key={`${hash}-${i}`} href={hash} title={source.text || undefined}>
            {label}
          </a>
        ) : (
          <span key={`${label}-${i}`} title={source.text || undefined}>
            {label}
          </span>
        );
      })}
    </div>
  );
}

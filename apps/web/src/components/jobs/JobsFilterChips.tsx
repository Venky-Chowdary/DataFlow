import { useMemo } from "react";
import type { JobVerificationFilter } from "../../lib/jobHistory";
import { jobEngineLabel } from "../../lib/jobGrid";
import { SYNC_MODES } from "../../lib/transferConstants";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";

export interface JobsFacets {
  sourceType: string;
  destinationType: string;
  syncMode: string;
  verification: "" | JobVerificationFilter;
}

export const EMPTY_JOBS_FACETS: JobsFacets = { sourceType: "", destinationType: "", syncMode: "", verification: "" };

export function hasJobsFacets(facets: JobsFacets): boolean {
  return Boolean(facets.sourceType || facets.destinationType || facets.syncMode || facets.verification);
}

const VERIFICATION_OPTIONS: { id: JobVerificationFilter; label: string }[] = [
  { id: "failed", label: "Gate-8 failed" },
  { id: "passed", label: "Gate-8 recorded, not failed" },
  { id: "none", label: "No Gate-8 verdict" },
];

interface JobsFilterChipsProps {
  facets: JobsFacets;
  onFacets: (facets: JobsFacets) => void;
  /** Engines seen in the loaded jobs — the options a filter can match. */
  sourceTypes: string[];
  destinationTypes: string[];
  dateActive: boolean;
  dateOpen: boolean;
  onToggleDate: () => void;
}

function withSelected(options: string[], selected: string): string[] {
  const all = new Set(options.filter(Boolean));
  if (selected) all.add(selected);
  return Array.from(all).sort((a, b) => jobEngineLabel(a).localeCompare(jobEngineLabel(b)));
}

/**
 * Second filter row: date range plus engine, sync-mode and verification
 * facets. Every facet is sent to the server, so paging and counts describe
 * the filtered history rather than the page in the browser.
 */
export function JobsFilterChips({
  facets,
  onFacets,
  sourceTypes,
  destinationTypes,
  dateActive,
  dateOpen,
  onToggleDate,
}: JobsFilterChipsProps) {
  const sources = useMemo(() => withSelected(sourceTypes, facets.sourceType), [sourceTypes, facets.sourceType]);
  const destinations = useMemo(
    () => withSelected(destinationTypes, facets.destinationType),
    [destinationTypes, facets.destinationType],
  );
  const set = (patch: Partial<JobsFacets>) => onFacets({ ...facets, ...patch });
  const active = hasJobsFacets(facets);

  return (
    <div className="df2-jobs-filter-chips" role="group" aria-label="Filter jobs">
      <Button
        size="sm"
        variant={dateOpen || dateActive ? "secondary" : "ghost"}
        aria-expanded={dateOpen}
        onClick={onToggleDate}
        leadingIcon={<DtIcon name="calendar" size={14} />}
      >
        {dateActive ? "Date range active" : "Date range"}
      </Button>
      <label className={`df2-jobs-filter-chip${facets.sourceType ? " is-active" : ""}`}>
        <span>Source</span>
        <select
          className="df2-input df2-select"
          aria-label="Source engine"
          value={facets.sourceType}
          onChange={(event) => set({ sourceType: event.target.value })}
        >
          <option value="">Any engine</option>
          {sources.map((id) => <option key={id} value={id}>{jobEngineLabel(id)}</option>)}
        </select>
      </label>
      <label className={`df2-jobs-filter-chip${facets.destinationType ? " is-active" : ""}`}>
        <span>Destination</span>
        <select
          className="df2-input df2-select"
          aria-label="Destination engine"
          value={facets.destinationType}
          onChange={(event) => set({ destinationType: event.target.value })}
        >
          <option value="">Any engine</option>
          {destinations.map((id) => <option key={id} value={id}>{jobEngineLabel(id)}</option>)}
        </select>
      </label>
      <label className={`df2-jobs-filter-chip${facets.syncMode ? " is-active" : ""}`}>
        <span>Mode</span>
        <select
          className="df2-input df2-select"
          aria-label="Sync mode"
          value={facets.syncMode}
          onChange={(event) => set({ syncMode: event.target.value })}
        >
          <option value="">Any mode</option>
          {SYNC_MODES.map((mode) => <option key={mode.id} value={mode.id}>{mode.label}</option>)}
        </select>
      </label>
      <label className={`df2-jobs-filter-chip${facets.verification ? " is-active" : ""}`}>
        <span>Verification</span>
        <select
          className="df2-input df2-select"
          aria-label="Verification"
          value={facets.verification}
          onChange={(event) => set({ verification: event.target.value as JobsFacets["verification"] })}
        >
          <option value="">Any result</option>
          {VERIFICATION_OPTIONS.map((option) => <option key={option.id} value={option.id}>{option.label}</option>)}
        </select>
      </label>
      {active && (
        <Button size="sm" variant="ghost" onClick={() => onFacets(EMPTY_JOBS_FACETS)}>
          Clear filters
        </Button>
      )}
    </div>
  );
}

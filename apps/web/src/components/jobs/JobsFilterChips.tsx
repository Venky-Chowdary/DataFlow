import { useMemo } from "react";
import type { JobVerificationFilter } from "../../lib/jobHistory";
import { jobEngineLabel } from "../../lib/jobGrid";
import { formatSyncModeLabel, SYNC_MODES } from "../../lib/transferConstants";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { JobsMenu, type JobsMenuItem } from "./JobsMenu";

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

const VERIFICATION_OPTIONS: { id: JobVerificationFilter; label: string; title: string }[] = [
  { id: "failed", label: "Failed", title: "Gate-8 verification failed" },
  { id: "passed", label: "Recorded", title: "A Gate-8 verdict was recorded and did not fail" },
  { id: "none", label: "Not run", title: "No Gate-8 verdict was recorded" },
];

/** UTC calendar day `daysAgo` before today, as the list's date filter reads it. */
export function utcDay(daysAgo = 0, now = new Date()): string {
  return new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - daysAgo)).toISOString().slice(0, 10);
}

const DATE_PRESETS = [
  { id: "today", label: "Today", from: () => utcDay(0) },
  { id: "7d", label: "Last 7 days", from: () => utcDay(6) },
  { id: "30d", label: "Last 30 days", from: () => utcDay(29) },
] as const;

/** The Date chip's value: a preset name, the custom range, or Any. */
export function dateChipValue(from: string, to: string): string {
  if (!from && !to) return "Any";
  if (!to) {
    const preset = DATE_PRESETS.find((p) => p.from() === from);
    if (preset) return preset.label;
  }
  if (from && to) return from === to ? from : `${from} – ${to}`;
  return from ? `From ${from}` : `Until ${to}`;
}

interface JobsFilterChipsProps {
  facets: JobsFacets;
  onFacets: (facets: JobsFacets) => void;
  /** Engines seen in the loaded jobs — the options a filter can match. */
  sourceTypes: string[];
  destinationTypes: string[];
  dateFrom: string;
  dateTo: string;
  onDates: (from: string, to: string) => void;
  /** Open the custom from/to panel. */
  onCustomDates: () => void;
}

function withSelected(options: string[], selected: string): string[] {
  const all = new Set(options.filter(Boolean));
  if (selected) all.add(selected);
  return Array.from(all).sort((a, b) => jobEngineLabel(a).localeCompare(jobEngineLabel(b)));
}

function Chip({
  name,
  value,
  active,
  items,
  onClear,
}: {
  name: string;
  value: string;
  active: boolean;
  items: JobsMenuItem[];
  onClear: () => void;
}) {
  const text = `${name}: ${value}`;
  return (
    <span className={`df2-jobs-chip${active ? " is-active" : ""}`}>
      <JobsMenu
        label={text}
        text={text}
        triggerClassName={`df2-chip df2-jobs-chip-trigger${active ? " active" : ""}`}
        trailingIcon={<DtIcon name="chevron-down" size={12} />}
        items={items}
      />
      {active && (
        <button
          type="button"
          className="df2-close-btn df2-close-btn-sm"
          aria-label={`Clear ${name} filter`}
          title={`Clear ${name} filter`}
          onClick={onClear}
        >
          <DtIcon name="x" size={13} />
        </button>
      )}
    </span>
  );
}

/**
 * Filter chips under the toolbar: Date, Source, Destination, Mode and
 * Verification. Each is a compact dropdown (the shared `df2-chip` look and
 * the jobs menu); an active chip shows its value and a ×. Every facet is
 * sent to the server, so paging and counts describe the filtered history.
 */
export function JobsFilterChips({
  facets,
  onFacets,
  sourceTypes,
  destinationTypes,
  dateFrom,
  dateTo,
  onDates,
  onCustomDates,
}: JobsFilterChipsProps) {
  const sources = useMemo(() => withSelected(sourceTypes, facets.sourceType), [sourceTypes, facets.sourceType]);
  const destinations = useMemo(
    () => withSelected(destinationTypes, facets.destinationType),
    [destinationTypes, facets.destinationType],
  );
  const set = (patch: Partial<JobsFacets>) => onFacets({ ...facets, ...patch });
  const dateActive = Boolean(dateFrom || dateTo);
  const radio = (id: string, label: string, checked: boolean, onSelect: () => void, title?: string): JobsMenuItem => ({
    id, label, checked, onSelect, title, kind: "radio",
  });
  const engineItems = (options: string[], selected: string, apply: (value: string) => void) => [
    radio("any", "Any", !selected, () => apply("")),
    ...options.map((id) => radio(id, jobEngineLabel(id), selected === id, () => apply(id))),
  ];

  return (
    <div className="df2-jobs-filter-chips" role="group" aria-label="Filter jobs">
      <Chip
        name="Date"
        value={dateChipValue(dateFrom, dateTo)}
        active={dateActive}
        onClear={() => onDates("", "")}
        items={[
          radio("any", "Any", !dateActive, () => onDates("", "")),
          ...DATE_PRESETS.map((preset) =>
            radio(preset.id, preset.label, !dateTo && dateFrom === preset.from(), () => onDates(preset.from(), ""))),
          { id: "custom", label: "Custom range…", onSelect: onCustomDates, title: "Pick from and to dates (UTC days)" },
        ]}
      />
      <Chip
        name="Source"
        value={facets.sourceType ? jobEngineLabel(facets.sourceType) : "Any"}
        active={Boolean(facets.sourceType)}
        onClear={() => set({ sourceType: "" })}
        items={engineItems(sources, facets.sourceType, (sourceType) => set({ sourceType }))}
      />
      <Chip
        name="Destination"
        value={facets.destinationType ? jobEngineLabel(facets.destinationType) : "Any"}
        active={Boolean(facets.destinationType)}
        onClear={() => set({ destinationType: "" })}
        items={engineItems(destinations, facets.destinationType, (destinationType) => set({ destinationType }))}
      />
      <Chip
        name="Mode"
        value={facets.syncMode ? formatSyncModeLabel(facets.syncMode) : "Any"}
        active={Boolean(facets.syncMode)}
        onClear={() => set({ syncMode: "" })}
        items={[
          radio("any", "Any", !facets.syncMode, () => set({ syncMode: "" })),
          ...SYNC_MODES.map((mode) => radio(mode.id, mode.label, facets.syncMode === mode.id, () => set({ syncMode: mode.id }), mode.detail)),
        ]}
      />
      <Chip
        name="Verification"
        value={VERIFICATION_OPTIONS.find((o) => o.id === facets.verification)?.label ?? "Any"}
        active={Boolean(facets.verification)}
        onClear={() => set({ verification: "" })}
        items={[
          radio("any", "Any", !facets.verification, () => set({ verification: "" })),
          ...VERIFICATION_OPTIONS.map((option) =>
            radio(option.id, option.label, facets.verification === option.id, () => set({ verification: option.id }), option.title)),
        ]}
      />
      {(hasJobsFacets(facets) || dateActive) && (
        <Button
          size="sm"
          variant="ghost"
          onClick={() => {
            onFacets(EMPTY_JOBS_FACETS);
            if (dateActive) onDates("", "");
          }}
        >
          Clear filters
        </Button>
      )}
    </div>
  );
}

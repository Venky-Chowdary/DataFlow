import { useCallback, useMemo, useState } from "react";
import {
  DEFAULT_JOBS_GRID_SORT,
  JOBS_GRID_COLUMNS,
  type JobsGridColumnId,
  type JobsGridDensity,
  type JobsGridPrefs,
  type JobsGridSort,
  jobsGridPrefsKey,
  readJobsGridPrefs,
  writeJobsGridPrefs,
} from "../../lib/jobGrid";
import type { TransferJob } from "../../lib/types";
import { EMPTY_JOBS_FACETS, type JobsFacets } from "./JobsFilterChips";

/** Facets applied to rows in hand (the recent feed when the page is not server-paged). */
export function applyJobsFacets(jobs: TransferJob[], facets: JobsFacets): TransferJob[] {
  if (!facets.sourceType && !facets.destinationType && !facets.syncMode && !facets.verification) return jobs;
  return jobs.filter((job) => {
    if (facets.sourceType && job.source_type !== facets.sourceType) return false;
    if (facets.destinationType && job.destination_type !== facets.destinationType) return false;
    if (facets.syncMode && job.sync_mode !== facets.syncMode) return false;
    if (facets.verification) {
      const passed = job.verification_summary?.passed;
      if (facets.verification === "failed" && passed !== false) return false;
      if (facets.verification === "passed" && passed !== true) return false;
      if (facets.verification === "none" && passed != null) return false;
    }
    return true;
  });
}

/**
 * Grid view state: engine/mode/verification facets, the local sort, and the
 * operator's remembered layout (visible columns, density). Layout persists per
 * signed-in user; facets and sort are per visit.
 */
export function useJobsGridState() {
  const [prefsKey] = useState(() => jobsGridPrefsKey());
  const [prefs, setPrefs] = useState<JobsGridPrefs>(() => readJobsGridPrefs(prefsKey));
  const [facets, setFacets] = useState<JobsFacets>(EMPTY_JOBS_FACETS);
  const [sort, setSort] = useState<JobsGridSort>(DEFAULT_JOBS_GRID_SORT);

  const updatePrefs = useCallback((next: JobsGridPrefs) => {
    setPrefs(next);
    writeJobsGridPrefs(next, prefsKey);
  }, [prefsKey]);

  const columns = useMemo(
    () => JOBS_GRID_COLUMNS.map((column) => column.id).filter((id) => !prefs.hidden.includes(id)),
    [prefs.hidden],
  );

  const toggleColumn = useCallback((column: JobsGridColumnId) => {
    const hidden = prefs.hidden.includes(column)
      ? prefs.hidden.filter((id) => id !== column)
      : [...prefs.hidden, column];
    updatePrefs({ ...prefs, hidden });
  }, [prefs, updatePrefs]);

  const setDensity = useCallback((density: JobsGridDensity) => {
    updatePrefs({ ...prefs, density });
  }, [prefs, updatePrefs]);

  const resetFacets = useCallback(() => setFacets(EMPTY_JOBS_FACETS), []);

  return {
    facets,
    setFacets,
    resetFacets,
    sort,
    setSort,
    density: prefs.density,
    setDensity,
    columns,
    toggleColumn,
  };
}

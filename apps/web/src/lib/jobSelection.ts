/**
 * Jobs list selection — persist the operator pick across list polls.
 * Deep-link focus applies once per job id; polling must not steal it.
 */

export function shouldApplyInitialJobFocus(
  initialJobId: string | undefined,
  lastApplied: string | null,
  jobIds: string[],
): boolean {
  if (!initialJobId) return false;
  if (lastApplied === initialJobId) return false;
  return jobIds.includes(initialJobId);
}

/** Keep the current pick when it is still in the filtered list. */
export function nextListSelection(
  selectedId: string | null,
  filteredIds: string[],
): string | null {
  if (filteredIds.length === 0) return selectedId;
  if (selectedId && filteredIds.includes(selectedId)) return selectedId;
  return filteredIds[0] ?? null;
}

/**
 * Leaving Job Theater for Validate after the run it shows has finished.
 *
 * A finished run — completed, quarantined, failed or stopped — spent the
 * verdict it launched on: the destination now holds that run's rows. Returning
 * to Validate for a rerun must ask for a fresh preflight and give Execute back,
 * not repaint the old gates beside the dead run's "Open live progress".
 * Only a run still in flight (no result yet) keeps its launch pointer.
 */
export type TheaterReturn = {
  /** Drop the launch pointer so the Validate primary is Execute again. */
  clearLaunch: boolean;
  /** Drop the spent verdict and its validated contract key. */
  clearVerdict: boolean;
  /** Re-run Validate against the destination as it is now. */
  rerunValidate: boolean;
};

export function theaterReturnToValidate(
  result: { success?: boolean } | null | undefined,
): TheaterReturn {
  const finished = Boolean(result);
  return { clearLaunch: finished, clearVerdict: finished, rerunValidate: finished };
}

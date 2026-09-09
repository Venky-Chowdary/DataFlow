/**
 * Transfer Studio Job Theater mount policy.
 *
 * Theater is the operator surface for live progress AND completed Gate-8.
 * Clearing the job id on terminal status unmounts Theater and hides the
 * Gate-8 card that only renders on `isComplete && job.reconciliation`.
 *
 * Leave Theater only via an explicit action (Validate / Map / New transfer).
 * Do not mount Theater and the result dashboard together — one Gate-8 card.
 */
import { isJobTerminal } from "./uiUtils";

/** Keep Theater mounted so Gate-8 / recovery CTAs stay on the operator path. */
export function keepTheaterMountedOnStatus(status: string | undefined): boolean {
  return isJobTerminal(status);
}

/** Route bar "live" is the write, not a completed Theater that is still mounted. */
export function routeBarLiveWhileWriting(transferring: boolean): boolean {
  return transferring;
}

/** Theater XOR result dashboard — never two Gate-8 cards on Execute. */
export function showStudioResultDashboard(input: {
  hasResult: boolean;
  theaterJobId: string | null | undefined;
}): boolean {
  return input.hasResult && !input.theaterJobId;
}

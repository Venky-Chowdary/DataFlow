import { useEffect, useState } from "react";

/** How long each progress step shows before the next (UX-42). */
export const PILOT_PROGRESS_STEP_MS = 1400;

const TRANSFER = /\b(copy|move|transfer|migrate|sync|replicate|load)\b/i;
const DATA = /\b(how many|count|rows?|sum|average|avg|total|top|sample|show me|list)\b/i;

/**
 * What Pilot is doing while it answers, step by step, from the first moment.
 * The steps follow what that kind of question costs on the server: every
 * answer first reads the workspace's connections.
 */
export function pilotProgressSteps(question: string): string[] {
  const q = question || "";
  if (TRANSFER.test(q)) {
    return [
      "Reading your connections…",
      "Reading the source and destination tables…",
      "Running preflight checks…",
      "Writing up the plan…",
    ];
  }
  if (DATA.test(q)) {
    return ["Reading your connections…", "Querying the table…", "Writing up the answer…"];
  }
  return ["Reading your connections…", "Looking through the documentation…", "Writing up the answer…"];
}

export function pilotProgressLabel(question: string, elapsedMs: number): string {
  const steps = pilotProgressSteps(question);
  const index = Math.min(steps.length - 1, Math.max(0, Math.floor(elapsedMs / PILOT_PROGRESS_STEP_MS)));
  return steps[index];
}

/** The current progress label while ``active``; restarts for each question. */
export function usePilotProgress(active: boolean, question: string): string {
  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    if (!active) {
      setElapsed(0);
      return;
    }
    const started = Date.now();
    setElapsed(0);
    const timer = window.setInterval(() => setElapsed(Date.now() - started), 250);
    return () => window.clearInterval(timer);
  }, [active, question]);
  return pilotProgressLabel(question, elapsed);
}

/**
 * Names the engine stage that is running, instead of "G1–G9".
 *
 * A client watching a validation run had no way to tell work from a hang: the
 * only signal was an internal gate range. This walks the nine core stages in the
 * order the engine runs them, so the wait reads as progress and every label is
 * something an engineer can act on. Reduced motion holds one line instead of
 * cycling; the whole ticker is one polite live region, not nine.
 */
import { useEffect, useState } from "react";
import { ENGINE_STAGES } from "../lib/preflightGates";

const HOLD_MS = 1100;

function prefersReducedMotion(): boolean {
  if (typeof window === "undefined" || !window.matchMedia) return false;
  return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

export function EngineStageTicker({
  running,
  className = "",
}: {
  running: boolean;
  className?: string;
}) {
  const [index, setIndex] = useState(0);
  const still = prefersReducedMotion();

  useEffect(() => {
    if (!running || still) return;
    const timer = window.setInterval(
      () => setIndex((i) => (i + 1) % ENGINE_STAGES.length),
      HOLD_MS,
    );
    return () => window.clearInterval(timer);
  }, [running, still]);

  useEffect(() => {
    if (!running) setIndex(0);
  }, [running]);

  if (!running) return null;
  const stage = ENGINE_STAGES[index] ?? ENGINE_STAGES[0];

  return (
    <span
      className={`df2-stage-ticker${className ? ` ${className}` : ""}`}
      aria-live="polite"
      aria-atomic="true"
    >
      <span className="df2-stage-ticker-count">
        {index + 1}/{ENGINE_STAGES.length}
      </span>
      <span className="df2-stage-ticker-name" key={stage.id}>
        {stage.running}…
      </span>
    </span>
  );
}

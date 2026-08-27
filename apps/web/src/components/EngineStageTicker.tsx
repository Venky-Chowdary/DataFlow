/**
 * Live Validate progress — rows scanned and wall clock.
 *
 * Do not cycle G1–G9. A completed stage does not come back. Reduced motion
 * still shows the same live counts (they are facts, not decoration).
 */
import { engineProgressCopy, type ValidateProgress } from "../lib/engineProgress";

export function EngineStageTicker({
  running,
  elapsedMs = 0,
  progress = null,
  className = "",
}: {
  running: boolean;
  elapsedMs?: number;
  progress?: ValidateProgress | null;
  className?: string;
}) {
  if (!running) return null;
  const copy = engineProgressCopy(progress, elapsedMs);

  return (
    <span
      className={`df2-stage-ticker${className ? ` ${className}` : ""}`}
      aria-live="polite"
      aria-atomic="true"
    >
      <span className="df2-stage-ticker-count">{copy.count}</span>
      <span className="df2-stage-ticker-name">{copy.name}</span>
    </span>
  );
}

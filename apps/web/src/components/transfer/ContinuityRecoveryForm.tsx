import { useId, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { useToast } from "../Toast";

export const CONTINUITY_GATE_ID = "g24_append_continuity";

/** Engine recovery prose names MCP tools and REST routes; the controls below replace it in the UI. */
export function stripContinuityRecoveryProse(text: string): string {
  return text.split(/\s+To recover:/)[0];
}

interface ContinuityRecoveryFormProps {
  /** The verified table is gone (vs. edited in place). */
  missing: boolean;
  /** Audited: moves (or clears, when missing) the append-continuity baseline. */
  onAccept?: (reason: string) => Promise<void>;
  /** Switch to full_refresh_overwrite and rebuild from the source. */
  onRebuild?: () => void;
}

/**
 * The two real ways out of an append-continuity block. Accepting needs a reason
 * because it is written to the audit log; rebuilding needs none because it
 * re-derives the destination from the source.
 */
export function ContinuityRecoveryForm({ missing, onAccept, onRebuild }: ContinuityRecoveryFormProps) {
  const { toast } = useToast();
  const inputId = useId();
  const [reason, setReason] = useState("");
  const [busy, setBusy] = useState(false);
  if (!onAccept && !onRebuild) return null;
  const trimmed = reason.trim();

  return (
    <form
      className="df2-continuity-recovery"
      data-testid="continuity-recovery"
      onSubmit={(e) => {
        e.preventDefault();
        if (!onAccept || !trimmed || busy) return;
        void (async () => {
          setBusy(true);
          try {
            await onAccept(trimmed);
          } catch (err) {
            toast({
              title: missing ? "Could not accept the removal" : "Could not accept the destination",
              message: (err as Error).message,
              tone: "error",
            });
          } finally {
            setBusy(false);
          }
        })();
      }}
    >
      {onAccept && (
        <>
          <label className="df2-field-label" htmlFor={inputId}>
            {missing
              ? "Table removed on purpose? Record why — the next append then starts a new verified history."
              : "Destination changed on purpose? Record why — it becomes the new verified baseline."}
          </label>
          <div className="df2-continuity-recovery-row">
            <input
              id={inputId}
              className="df2-input"
              value={reason}
              maxLength={2000}
              placeholder="Reason for the audit log, e.g. INC-142 table rebuilt by DBA"
              onChange={(e) => setReason(e.target.value)}
            />
            <Button
              type="submit"
              size="sm"
              variant="primary"
              disabled={!trimmed || busy}
              leadingIcon={<DtIcon name="shield" size={14} />}
            >
              {busy ? "Recording…" : missing ? "Accept removal" : "Accept current destination"}
            </Button>
          </div>
        </>
      )}
      <div className="df2-continuity-recovery-row">
        {onAccept && !trimmed && (
          <p className="df2-field-hint">A reason is required before the baseline can move.</p>
        )}
        {onRebuild && (
          <Button
            type="button"
            size="sm"
            variant="secondary"
            disabled={busy}
            leadingIcon={<DtIcon name="refresh" size={14} />}
            onClick={onRebuild}
          >
            Rebuild with full refresh
          </Button>
        )}
      </div>
    </form>
  );
}

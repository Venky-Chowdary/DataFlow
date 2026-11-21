import { useId, useState } from "react";
import { DtIcon } from "../DtIcon";
import { FLAG_LABEL, type WitnessPair } from "../../lib/mappingWitness";

/**
 * One compact flag on a Map row: the Mapping Witness found data that
 * contradicts this pair. The chip discloses why and offers Review (the row's
 * destination picker) and Keep this mapping (an explicit, recorded answer to
 * this flag). The witness never changes the mapping itself.
 */
export function WitnessFlag({
  pair,
  onReview,
  onKeep,
  kept = false,
}: {
  pair: WitnessPair;
  onReview?: (source: string) => void;
  /** "Keep this mapping": an explicit answer to this flag, recorded with its id. */
  onKeep?: (pair: WitnessPair) => Promise<void> | void;
  kept?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [keepError, setKeepError] = useState("");
  const whyId = useId();
  if (!pair.flag) return null;
  const label = kept ? "Kept after review" : FLAG_LABEL[pair.flag.kind] ?? "Check values";
  return (
    <span className="df2-witness">
      <button
        type="button"
        className={`df2-witness-chip${kept ? " is-kept" : ""}`}
        aria-expanded={open}
        aria-controls={whyId}
        onClick={() => setOpen((v) => !v)}
        data-kind={pair.flag.kind}
      >
        <DtIcon name="alert" size={14} chrome="product" />
        {label}
      </button>
      {open && (
        <span id={whyId} className="df2-witness-why" role="note">
          <span className="df2-witness-reason">{pair.flag.reason}</span>
          {onReview && (
            <button
              type="button"
              className="df2-btn df2-btn-ghost df2-btn-xs df2-witness-review"
              onClick={() => onReview(pair.source)}
            >
              Review
            </button>
          )}
          {onKeep && !kept && (
            <button
              type="button"
              className="df2-btn df2-btn-ghost df2-btn-xs df2-witness-keep"
              disabled={saving}
              onClick={async () => {
                setSaving(true);
                setKeepError("");
                try {
                  await onKeep(pair);
                } catch (e) {
                  setKeepError(e instanceof Error ? e.message : "The review could not be recorded");
                } finally {
                  setSaving(false);
                }
              }}
            >
              Keep this mapping
            </button>
          )}
          {keepError && (
            <span className="df2-witness-error" role="alert">
              {keepError}
            </span>
          )}
        </span>
      )}
    </span>
  );
}

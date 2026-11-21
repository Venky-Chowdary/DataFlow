import { useId, useState } from "react";
import { DtIcon } from "../DtIcon";
import { FLAG_LABEL, type WitnessPair } from "../../lib/mappingWitness";

/**
 * One compact flag on a Map row: the Mapping Witness found data that
 * contradicts this pair. The chip discloses why; the only action is Review,
 * which takes the operator to the row's destination picker. The witness never
 * changes the mapping itself.
 */
export function WitnessFlag({ pair, onReview }: { pair: WitnessPair; onReview?: (source: string) => void }) {
  const [open, setOpen] = useState(false);
  const whyId = useId();
  if (!pair.flag) return null;
  const label = FLAG_LABEL[pair.flag.kind] ?? "Check values";
  return (
    <span className="df2-witness">
      <button
        type="button"
        className="df2-witness-chip"
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
        </span>
      )}
    </span>
  );
}

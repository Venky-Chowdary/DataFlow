import { DtIcon } from "../DtIcon";
import type { ReplaySafetyReport } from "../../lib/types";

/**
 * Tells the operator whether an interrupted write can be retried in place.
 *
 * Without this card, a transfer that failed mid-batch looks identical to one
 * that can be safely resumed — until a retry doubles the destination. The
 * verdict comes from the engine's own classification of the write mode and the
 * destination's durable chunk ledger (or upsert keys).
 */
export function ReplaySafetyCard({ report }: { report?: ReplaySafetyReport | null }) {
  if (!report || !report.mechanism) return null;

  const safe = Boolean(report.safe);
  const mechanismLabel = MECHANISM_LABEL[report.mechanism] || report.mechanism;
  const tone = safe ? "ok" : "warn";

  return (
    <section
      className={`df2-result-replay df2-result-replay--${tone}`}
      aria-label="Write replay safety"
    >
      <header>
        <DtIcon name={safe ? "shield" : "alert"} size={14} />
        <strong>{safe ? "Retries are safe" : "Retries risk duplicates"}</strong>
        <span>{mechanismLabel}</span>
      </header>
      <p>{report.reason}</p>
      {!safe && (
        <footer>
          Resume from the last committed chunk, or switch the sync mode to upsert
          with a primary key so a mid-batch failure can be retried automatically.
        </footer>
      )}
    </section>
  );
}

const MECHANISM_LABEL: Record<string, string> = {
  idempotent_upsert: "Keyed upsert",
  chunk_ledger: "Committed-chunk ledger",
  keyed_document: "Document id",
  none: "Append-only",
};

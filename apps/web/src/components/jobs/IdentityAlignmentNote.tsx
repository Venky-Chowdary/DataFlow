import {
  identityAlignmentSentence,
  identityAlignmentTone,
  readIdentityAlignment,
  type JobEvidenceCarrier,
} from "../../lib/jobEvidence";

/**
 * Write-pass versus independent re-read. The engine stamps this on
 * destination_summary.identity_alignment. A skipped fingerprint is not a mismatch.
 */
export function IdentityAlignmentNote({ job }: { job: JobEvidenceCarrier | null | undefined }) {
  const view = readIdentityAlignment(job);
  if (!view) return null;
  const tone = identityAlignmentTone(view);
  return (
    <section
      className={tone === "warn" ? "df2-result-warnings-block" : "df2-jobs-overview-note"}
      role="status"
      aria-label="Identity alignment"
    >
      <p className={tone === "warn" ? "df2-result-warnings-note" : undefined}>
        {identityAlignmentSentence(view)}
      </p>
    </section>
  );
}

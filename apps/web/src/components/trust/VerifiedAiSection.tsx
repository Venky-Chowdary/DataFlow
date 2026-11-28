import { AiInvolvementSection } from "./AiInvolvementSection";
import { MappingEvidenceSection } from "./MappingEvidenceSection";

/**
 * Verified AI evidence for one job, as the Trust Report shows it. Self-contained:
 * each section loads its own data, so the report layout can place it anywhere.
 */
export function VerifiedAiSection({ jobId }: { jobId: string }) {
  if (!jobId) return null;
  return (
    <>
      <MappingEvidenceSection jobId={jobId} />
      <AiInvolvementSection jobId={jobId} />
    </>
  );
}

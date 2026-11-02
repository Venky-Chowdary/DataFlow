/**
 * Proofs → Scale: "Datawrap vs this" from the two numbers (R7).
 *
 * QA saw "5.6x slower" on a competitor row listing 4,000 rows/s against our
 * 717 — the badge named no subject and read as if the competitor were slower.
 * The label always names Datawrap and follows the ratio.
 */
export const SIMILAR_BAND = 0.1;

export type ThroughputVerdict = "faster" | "slower" | "similar" | "unknown";

export function compareThroughput(datawrapRps: number, otherRps: number): { verdict: ThroughputVerdict; label: string } {
  const ours = Math.max(Number(datawrapRps) || 0, 0);
  const theirs = Math.max(Number(otherRps) || 0, 0);
  if (!ours || !theirs) return { verdict: "unknown", label: "No measurement" };
  const ratio = ours / theirs;
  if (Math.abs(ratio - 1) <= SIMILAR_BAND) return { verdict: "similar", label: "About the same" };
  return ratio > 1
    ? { verdict: "faster", label: `Datawrap ${ratio.toFixed(1)}× faster` }
    : { verdict: "slower", label: `Datawrap ${(1 / ratio).toFixed(1)}× slower` };
}

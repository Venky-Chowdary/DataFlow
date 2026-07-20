/** Helpers for multi-stream Map rematch (divergent schemas). */

export function streamColumnSignature(columns: string[] | undefined | null): string {
  return [...(columns || [])].map((c) => c.toLowerCase()).sort().join("|");
}

export function streamsNeedPerStreamRematch(
  streamPreviews: { name: string; columns?: string[] }[],
): boolean {
  const ok = streamPreviews.filter((s) => (s.columns?.length ?? 0) > 0);
  if (ok.length < 2) return false;
  const first = streamColumnSignature(ok[0].columns);
  return ok.some((s) => streamColumnSignature(s.columns) !== first);
}

export function missingStreamMappings(
  streamNames: string[],
  streamMappings: Record<string, unknown[] | undefined>,
  activeStream: string,
  activeMappings: unknown[],
): string[] {
  const merged: Record<string, unknown[]> = {};
  for (const [k, v] of Object.entries(streamMappings)) {
    if (Array.isArray(v)) merged[k] = v;
  }
  merged[activeStream] = activeMappings;
  return streamNames.filter((n) => !(merged[n]?.length));
}

import { useEffect, useMemo, useState } from "react";
import { fetchJobVerdicts } from "../../lib/api";
import type { TransferJob } from "../../lib/types";
import {
  listVerdictStamp,
  mergeListVerdicts,
  verdictIdsToFetch,
  type CachedListVerdict,
} from "../../lib/verdict";

/**
 * The Trust Report verdict of each finished row on the current Jobs page
 * (one batch read per page, re-read when a row's job state changes). Rows
 * are returned with `parity_verdict` merged in.
 */
export function useJobVerdicts(jobs: TransferJob[]): TransferJob[] {
  const [cache, setCache] = useState<ReadonlyMap<string, CachedListVerdict>>(() => new Map());
  const wanted = useMemo(() => verdictIdsToFetch(jobs, cache), [jobs, cache]);
  const key = wanted.join(",");
  useEffect(() => {
    if (!key) return;
    const ids = key.split(",");
    const stamps = new Map(jobs.filter((job) => ids.includes(job._id)).map((job) => [job._id, listVerdictStamp(job)]));
    const controller = new AbortController();
    const settle = (verdicts: Record<string, CachedListVerdict["verdict"]>) => {
      setCache((prev) => {
        const next = new Map(prev);
        for (const id of ids) next.set(id, { stamp: stamps.get(id) ?? "", verdict: verdicts[id] ?? null });
        return next;
      });
    };
    fetchJobVerdicts(ids, { signal: controller.signal })
      .then(settle)
      .catch(() => {
        if (!controller.signal.aborted) settle({});
      });
    return () => controller.abort();
    // `jobs` is read for the stamps of exactly the ids in `key`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);
  return useMemo(() => mergeListVerdicts(jobs, cache), [jobs, cache]);
}

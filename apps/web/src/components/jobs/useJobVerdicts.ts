import { useEffect, useMemo, useState } from "react";
import { fetchJobVerdicts } from "../../lib/api";
import { useReportVerdicts } from "../../lib/reportVerdicts";
import type { TransferJob } from "../../lib/types";
import {
  listVerdictStamp,
  mergeListVerdicts,
  verdictIdsToFetch,
  type CachedListVerdict,
} from "../../lib/verdict";

/** A failed read is retried after this long (it is never cached). */
export const VERDICT_RETRY_MS = 15_000;

/**
 * The Trust Report verdict of each finished row on the current Jobs page:
 * one batch read per page, re-read when a row's job or parity state changes,
 * and overridden by any Trust Report read in this session that is newer
 * (review UI2 #2). Rows are returned with `parity_verdict` merged in.
 */
export function useJobVerdicts(jobs: TransferJob[]): TransferJob[] {
  const [cache, setCache] = useState<ReadonlyMap<string, CachedListVerdict>>(() => new Map());
  const [unavailable, setUnavailable] = useState<ReadonlySet<string>>(() => new Set());
  const [retry, setRetry] = useState(0);
  const reports = useReportVerdicts();
  const wanted = useMemo(() => verdictIdsToFetch(jobs, cache), [jobs, cache]);
  // Ids with the job state they are read for: a changed state asks again even after a failed read.
  const key = useMemo(() => {
    const byId = new Map(jobs.map((job) => [job._id, job]));
    return wanted.map((id) => `${id}\u0001${byId.get(id) ? listVerdictStamp(byId.get(id)!) : ""}`).join("\u0002");
  }, [wanted, jobs]);
  useEffect(() => {
    if (!key) return;
    const ids = key.split("\u0002").map((part) => part.split("\u0001")[0]);
    const stamps = new Map(jobs.filter((job) => ids.includes(job._id)).map((job) => [job._id, listVerdictStamp(job)]));
    const controller = new AbortController();
    let timer: number | undefined;
    fetchJobVerdicts(ids, { signal: controller.signal })
      .then((verdicts) => {
        const at = Date.now();
        setCache((prev) => {
          const next = new Map(prev);
          for (const id of ids) if (verdicts[id]) next.set(id, { stamp: stamps.get(id) ?? "", verdict: verdicts[id], at });
          return next;
        });
        // Left out by the API (not visible, not finished): shown as not loaded, never cached.
        setUnavailable(new Set(ids.filter((id) => !verdicts[id])));
      })
      .catch(() => {
        if (controller.signal.aborted) return;
        setUnavailable(new Set(ids));
        timer = window.setTimeout(() => setRetry((n) => n + 1), VERDICT_RETRY_MS);
      });
    return () => {
      controller.abort();
      if (timer !== undefined) window.clearTimeout(timer);
    };
    // `jobs` is read for the stamps of exactly the ids in `key`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, retry]);
  return useMemo(() => mergeListVerdicts(jobs, cache, reports, unavailable), [jobs, cache, reports, unavailable]);
}

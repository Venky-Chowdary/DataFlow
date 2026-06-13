import { useEffect, useRef, useState } from "react";
import { fetchJob, type JobDetail } from "./api";

interface UseJobPollOptions {
  intervalMs?: number;
  enabled?: boolean;
  preferStream?: boolean;
}

export function useJobPoll(jobId: string | null, options: UseJobPollOptions = {}) {
  const { intervalMs = 400, enabled = true, preferStream = true } = options;
  const [job, setJob] = useState<JobDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const doneRef = useRef(false);

  useEffect(() => {
    if (!jobId || !enabled) return;
    doneRef.current = false;
    let cancelled = false;
    let es: EventSource | null = null;

    function finish(data: JobDetail) {
      setJob(data);
      if (data.status === "completed" || data.status === "failed") {
        doneRef.current = true;
      }
    }

    async function pollLoop() {
      while (!cancelled && !doneRef.current) {
        try {
          const data = await fetchJob(jobId!);
          if (cancelled) return;
          finish(data);
          if (doneRef.current) return;
        } catch (e) {
          if (!cancelled) setError(e instanceof Error ? e.message : "Poll failed");
          return;
        }
        await new Promise((r) => setTimeout(r, intervalMs));
      }
    }

    if (preferStream && typeof EventSource !== "undefined") {
      es = new EventSource(`/api/v1/jobs/${jobId}/stream`);
      es.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data) as JobDetail;
          if (cancelled) return;
          finish(data);
          if (doneRef.current) es?.close();
        } catch {
          es?.close();
          pollLoop();
        }
      };
      es.onerror = () => {
        es?.close();
        if (!cancelled && !doneRef.current) pollLoop();
      };
    } else {
      pollLoop();
    }

    return () => {
      cancelled = true;
      es?.close();
    };
  }, [jobId, enabled, intervalMs, preferStream]);

  return { job, error, isComplete: job?.status === "completed", isFailed: job?.status === "failed" };
}

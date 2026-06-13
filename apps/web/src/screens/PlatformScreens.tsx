import {
  Button,
  JobDetailPanel,
  JobList,
  LoadingState,
  PreflightGateRail,
  StatStrip,
  StatusTileGrid,
  type JobDetailData,
  type StatusTile,
} from "@dataflow/design-system";
import { useCallback, useEffect, useState } from "react";
import {
  fetchGateStats,
  fetchJob,
  fetchJobs,
  fetchPlatformStats,
  type GateStatTile,
  type JobDetail,
  type JobListItem,
  type PlatformStats,
} from "../lib/api";
import { useJobPoll } from "../lib/useJobPoll";

interface ScreenCtaProps {
  onNewTransfer?: () => void;
}

function gateTilesFromApi(gates: GateStatTile[]): StatusTile[] {
  return gates.map((g) => ({
    id: g.id,
    label: g.label,
    count: g.count,
    status: (["active", "warning", "broken", "idle"].includes(g.status)
      ? g.status
      : "idle") as StatusTile["status"],
  }));
}

/** Operations console — run history, throughput, gate pass rates */
export function OperationsScreen({ onNewTransfer }: ScreenCtaProps) {
  const [stats, setStats] = useState<PlatformStats | null>(null);
  const [gateTiles, setGateTiles] = useState<StatusTile[]>([]);
  const [jobs, setJobs] = useState<JobListItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<JobDetail | null>(null);

  const activeJob = selectedId && detail?.status === "running" ? selectedId : null;
  const { job: polled } = useJobPoll(activeJob, { enabled: !!activeJob });

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [s, j, g] = await Promise.all([fetchPlatformStats(), fetchJobs(50), fetchGateStats()]);
      setStats(s);
      setJobs(j);
      setGateTiles(gateTilesFromApi(g.gates));
    } catch {
      setStats(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    if (polled) setDetail(polled);
  }, [polled]);

  const loadDetail = useCallback(async (jobId: string) => {
    setSelectedId(jobId);
    try {
      setDetail(await fetchJob(jobId));
    } catch {
      setDetail(null);
    }
  }, []);

  useEffect(() => {
    if (detail?.status === "completed" || detail?.status === "failed") {
      fetchJobs(50).then(setJobs).catch(() => {});
    }
  }, [detail?.status]);

  return (
    <>
      <p className="df-page-desc">
        Monitor transfer jobs, row throughput, and preflight gate pass rates across the platform.
      </p>

      {loading ? (
        <LoadingState label="Loading operations data…" compact />
      ) : (
        <StatStrip
          items={[
            { label: "Total runs", value: stats?.total_jobs ?? 0 },
            { label: "Rows transferred", value: stats?.rows_transferred ?? 0, tone: "emerald" },
            { label: "Active", value: stats?.active ?? 0 },
            { label: "Failed", value: stats?.failed ?? 0, tone: "amber" },
          ]}
        />
      )}

      <PreflightGateRail passedCount={gateTiles.filter((t) => t.status === "active").length} />

      <div className={["df-jobs-layout", detail ? "df-jobs-layout--split" : ""].filter(Boolean).join(" ")}>
        <section className="df-section">
          <div className="df-section-head">
            <span className="df-section-title">Transfer jobs</span>
            <div className="df-section-head-actions">
              <Button variant="ghost" onClick={load} disabled={loading}>
                Refresh
              </Button>
              {onNewTransfer && (
                <Button variant="primary" onClick={onNewTransfer}>
                  New transfer
                </Button>
              )}
            </div>
          </div>
          <div className="df-section-surface">
            {loading ? (
              <LoadingState label="Loading jobs…" compact />
            ) : jobs.length === 0 ? (
              <p className="df-empty-inline">No transfer jobs yet. Start from the Transfer workspace.</p>
            ) : (
              <JobList jobs={jobs} selectedId={selectedId} onSelect={loadDetail} />
            )}
          </div>
        </section>

        <section className="df-section">
          <div className="df-section-head">
            <span className="df-section-title">Gate pass rates</span>
          </div>
          <div className="df-section-surface df-section-surface--padded">
            {loading ? (
              <LoadingState label="Loading gates…" compact />
            ) : gateTiles.length > 0 ? (
              <StatusTileGrid tiles={gateTiles} />
            ) : (
              <p className="df-empty-inline">Complete a transfer to populate gate statistics.</p>
            )}
          </div>
        </section>
      </div>

      {detail && (
        <section className="df-section">
          <div className="df-section-head">
            <span className="df-section-title">Job detail</span>
          </div>
          <JobDetailPanel
            job={
              {
                ...detail,
                checkpoints: detail.checkpoints ?? [],
                reconciliation: detail.reconciliation,
              } as JobDetailData
            }
          />
        </section>
      )}
    </>
  );
}

export { ConnectorsScreen } from "./ConnectorsScreen";

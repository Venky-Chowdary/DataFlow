import { useCallback, useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import {
  cancelJobCdcSnapshot,
  listJobCdcSnapshots,
  requestJobCdcSnapshot,
  type CdcSnapshotSignal,
} from "../../lib/api";
import { useToast } from "../Toast";

export interface CdcIncrementalSnapshotPanelProps {
  jobId: string;
  /** Prefer showing for CDC jobs (plugin / watermark / delivery). */
  enabled?: boolean;
  defaultTable?: string;
  defaultPrimaryKey?: string;
  /** Poll while pending/running (ms). */
  pollMs?: number;
}

/**
 * Debezium-style incremental snapshot control — request / cancel / monitor
 * backfill interleaved with CDC. Uses job-scoped API so source_key matches
 * the live CDC fingerprint (no invented keys).
 */
export function CdcIncrementalSnapshotPanel({
  jobId,
  enabled = true,
  defaultTable = "",
  defaultPrimaryKey = "id",
  pollMs = 4000,
}: CdcIncrementalSnapshotPanelProps) {
  const { toast } = useToast();
  const [signals, setSignals] = useState<CdcSnapshotSignal[]>([]);
  const [table, setTable] = useState(defaultTable);
  const [primaryKey, setPrimaryKey] = useState(defaultPrimaryKey || "id");
  const [chunkSize, setChunkSize] = useState(1000);
  const [honesty, setHonesty] = useState("");
  const [sourceKey, setSourceKey] = useState("");
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Reset local state when the operator switches jobs.
  useEffect(() => {
    setSignals([]);
    setTable(defaultTable);
    setPrimaryKey(defaultPrimaryKey || "id");
    setHonesty("");
    setSourceKey("");
    setError(null);
  }, [jobId, defaultTable, defaultPrimaryKey]);

  const refresh = useCallback(async () => {
    if (!jobId || !enabled) return;
    setLoading(true);
    setError(null);
    try {
      const res = await listJobCdcSnapshots(jobId);
      setSignals(res.signals || []);
      setHonesty(res.context?.honesty || "");
      setSourceKey(res.context?.source_key || "");
      setTable((prev) => prev || res.context?.table || "");
      setPrimaryKey((prev) => {
        if (prev && prev !== "id") return prev;
        return res.context?.primary_key || prev || "id";
      });
    } catch (e) {
      setError((e as Error).message || "Could not load snapshot signals");
    } finally {
      setLoading(false);
    }
  }, [jobId, enabled]);

  useEffect(() => {
    void refresh();
  }, [jobId, enabled]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!enabled || !jobId) return;
    const active = signals.some((s) => s.status === "pending" || s.status === "running");
    if (!active) return;
    const t = window.setInterval(() => void refresh(), pollMs);
    return () => window.clearInterval(t);
  }, [enabled, jobId, signals, pollMs, refresh]);

  if (!enabled || !jobId) return null;

  const request = async () => {
    setBusy(true);
    try {
      const sig = await requestJobCdcSnapshot(jobId, {
        table: table || undefined,
        primary_key: primaryKey || undefined,
        chunk_size: chunkSize,
      });
      toast({
        title: "Incremental snapshot requested",
        message: `${sig.table} · ${sig.id} — CDC will interleave PK chunks (at-least-once upsert).`,
        tone: "success",
      });
      await refresh();
    } catch (e) {
      toast({ title: "Request failed", message: (e as Error).message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  const cancel = async (signalId: string) => {
    setBusy(true);
    try {
      await cancelJobCdcSnapshot(jobId, signalId);
      toast({ title: "Snapshot cancelled", message: signalId, tone: "warning" });
      await refresh();
    } catch (e) {
      toast({ title: "Cancel failed", message: (e as Error).message, tone: "error" });
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="df2-cdc-snapshot-panel" aria-label="CDC incremental snapshot">
      <header className="df2-cdc-snapshot-head">
        <DtIcon name="layers" size={16} />
        <div>
          <strong>Incremental snapshot</strong>
          <span>
            Debezium-style backfill without cutover — chunks interleaved with CDC
            (stream-wins). Not exactly-once; not destination undo.
          </span>
        </div>
        <Button variant="ghost" size="sm" onClick={() => void refresh()} disabled={loading || busy}>
          {loading ? "Refreshing…" : "Refresh"}
        </Button>
      </header>

      {honesty && <p className="df2-label-hint">{honesty}</p>}
      {sourceKey && (
        <p className="df2-label-hint">
          Source key <code className="df2-mono">{sourceKey}</code>
        </p>
      )}
      {error && (
        <p className="df2-label-hint" role="alert" style={{ color: "var(--df2-danger, #b42318)" }}>
          {error}
        </p>
      )}

      <div className="df2-form-row" style={{ gap: "0.5rem", flexWrap: "wrap" }}>
        <div className="df2-field" style={{ flex: "1 1 8rem" }}>
          <label className="df2-label" htmlFor={`cdc-snap-table-${jobId}`}>Table</label>
          <input
            id={`cdc-snap-table-${jobId}`}
            className="df2-input"
            value={table}
            onChange={(e) => setTable(e.target.value)}
            placeholder="orders"
          />
        </div>
        <div className="df2-field" style={{ flex: "0 1 6rem" }}>
          <label className="df2-label" htmlFor={`cdc-snap-pk-${jobId}`}>Primary key</label>
          <input
            id={`cdc-snap-pk-${jobId}`}
            className="df2-input"
            value={primaryKey}
            onChange={(e) => setPrimaryKey(e.target.value)}
            placeholder="id"
          />
        </div>
        <div className="df2-field" style={{ flex: "0 1 5rem" }}>
          <label className="df2-label" htmlFor={`cdc-snap-chunk-${jobId}`}>Chunk</label>
          <input
            id={`cdc-snap-chunk-${jobId}`}
            className="df2-input"
            type="number"
            min={1}
            max={50000}
            value={chunkSize}
            onChange={(e) => setChunkSize(Math.max(1, Number(e.target.value) || 1000))}
          />
        </div>
        <div className="df2-field" style={{ alignSelf: "flex-end" }}>
          <Button
            variant="primary"
            size="sm"
            onClick={() => void request()}
            loading={busy}
            loadingLabel="Requesting…"
            disabled={!table.trim()}
            leadingIcon={<DtIcon name="transfer" size={14} />}
          >
            Request snapshot
          </Button>
        </div>
      </div>

      {signals.length === 0 ? (
        <p className="df2-label-hint">No snapshot signals for this source yet.</p>
      ) : (
        <ul className="df2-cdc-snapshot-list">
          {signals.slice(0, 8).map((s) => (
            <li key={s.id}>
              <div>
                <strong className="df2-mono">{s.table}</strong>
                <span className={`df2-cdc-snap-status is-${s.status}`}>{s.status}</span>
                <span className="df2-label-hint">
                  {s.rows_snapshotted?.toLocaleString?.() ?? s.rows_snapshotted} rows
                  {s.last_pk ? ` · last_pk ${s.last_pk}` : ""}
                </span>
                {s.error ? <span className="df2-label-hint" role="alert">{s.error}</span> : null}
                <code className="df2-mono df2-label-hint">{s.id}</code>
              </div>
              {(s.status === "pending" || s.status === "running") && (
                <Button variant="secondary" size="sm" onClick={() => void cancel(s.id)} disabled={busy}>
                  Cancel
                </Button>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

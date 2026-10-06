import { destHeadline, destMetricCompact } from "../../lib/conservationLedger";
import type { CdcStreamHealth } from "../../lib/types";

/** One table for per-stream dest COUNT, lag, and watermark. */
export function StreamHealthTable({ streams }: { streams: CdcStreamHealth[] }) {
  return (
    <table className="df2-table df2-jobs-cdc-table">
      <thead>
        <tr>
          <th>Stream</th>
          <th>Status</th>
          <th>Conserved</th>
          <th>Events written</th>
          <th>Lag</th>
          <th>Watermark</th>
        </tr>
      </thead>
      <tbody>
        {streams.map((stream) => (
          <tr key={stream.name}>
            <td>{stream.name}</td>
            <td>
              {stream.status || "—"}
              {stream.error ? <div className="df2-muted">{stream.error}</div> : null}
            </td>
            <td>
              {destMetricCompact(
                destHeadline({
                  status: stream.status,
                  records_processed: stream.records_processed,
                  row_accounting: stream.row_accounting,
                }),
              )}
            </td>
            <td>{Number(stream.records_processed ?? 0).toLocaleString()}</td>
            <td>
              {stream.cdc_lag_seconds != null && Number.isFinite(Number(stream.cdc_lag_seconds))
                ? `${Number(stream.cdc_lag_seconds).toFixed(1)}s`
                : "—"}
            </td>
            <td className="df2-cell-mono" title={stream.watermark || ""}>
              {stream.watermark
                ? `${String(stream.watermark).slice(0, 40)}${String(stream.watermark).length > 40 ? "…" : ""}`
                : "—"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

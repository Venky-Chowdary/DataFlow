import { DtIcon } from "../DtIcon";
import type { ConnectionReuseReport, ReuseCounters } from "../../lib/types";

/**
 * Proof that a transfer reused its connection pools and schema lookups.
 *
 * Without this card the operator has no way to tell a transfer that opened
 * one pooled connection across N chunks from one that rebuilt a pool per
 * chunk. The numbers come from the engine's own hit/miss counters for this
 * run — not a marketing claim.
 */
export function ConnectionReuseCard({
  report,
  traceId,
  correlationId,
}: {
  report?: ConnectionReuseReport | null;
  traceId?: string | null;
  correlationId?: string | null;
}) {
  const pool = report?.engine_pool;
  const schema = report?.schema_cache;
  const hasReuse =
    (pool && ((pool.hits ?? 0) > 0 || (pool.misses ?? 0) > 0)) ||
    (schema && ((schema.hits ?? 0) > 0 || (schema.misses ?? 0) > 0));
  const hasTrace = Boolean(traceId || correlationId);
  if (!hasReuse && !hasTrace) return null;

  return (
    <section className="df2-result-reuse" aria-label="Connection reuse and trace">
      <header>
        <DtIcon name="refresh" size={14} />
        <strong>Connection reuse</strong>
        {hasReuse && (
          <span>
            {summarize(pool, "engines")}
            {schema && (schema.hits ?? 0) + (schema.misses ?? 0) > 0
              ? ` · ${summarize(schema, "metadata")}`
              : ""}
          </span>
        )}
      </header>

      {hasReuse && (
        <ul className="df2-result-reuse-list">
          {pool && ((pool.hits ?? 0) + (pool.misses ?? 0) > 0) && (
            <li>
              <ReuseMeter label="Engine pool" counters={pool} unit="connections saved" />
            </li>
          )}
          {schema && ((schema.hits ?? 0) + (schema.misses ?? 0) > 0) && (
            <li>
              <ReuseMeter
                label="Schema cache"
                counters={schema}
                unit="metadata queries saved"
              />
            </li>
          )}
        </ul>
      )}

      {hasTrace && (
        <footer className="df2-result-reuse-trace">
          {traceId && (
            <code title="OpenTelemetry trace id" className="df2-result-trace-id">
              trace {shortId(traceId)}
            </code>
          )}
          {correlationId && (
            <code title="Request correlation id" className="df2-result-trace-id">
              corr {shortId(correlationId)}
            </code>
          )}
        </footer>
      )}
    </section>
  );
}

function ReuseMeter({
  label,
  counters,
  unit,
}: {
  label: string;
  counters: ReuseCounters;
  unit: string;
}) {
  const hits = counters.hits ?? 0;
  const misses = counters.misses ?? 0;
  const total = hits + misses;
  const pct = total > 0 ? Math.round((hits / total) * 100) : 0;
  const saved = counters.connections_saved ?? counters.metadata_queries_saved ?? hits;
  return (
    <div className="df2-result-reuse-meter">
      <div className="df2-result-reuse-head">
        <span>{label}</span>
        <span>
          {pct}% reused · {saved.toLocaleString()} {unit}
        </span>
      </div>
      <div
        className="df2-result-reuse-bar"
        role="img"
        aria-label={`${label}: ${pct}% reused`}
      >
        <span style={{ width: `${Math.max(pct, total > 0 ? 1.5 : 0)}%` }} />
      </div>
      <div className="df2-result-reuse-meta">
        <span>{hits.toLocaleString()} hits</span>
        <span aria-hidden="true">·</span>
        <span>{misses.toLocaleString()} misses</span>
      </div>
    </div>
  );
}

function summarize(counters: ReuseCounters | undefined, kind: string): string {
  if (!counters) return "";
  const hits = counters.hits ?? 0;
  const misses = counters.misses ?? 0;
  const total = hits + misses;
  if (total === 0) return "";
  const pct = Math.round((hits / total) * 100);
  return `${pct}% ${kind} reused`;
}

function shortId(value: string): string {
  if (value.length <= 16) return value;
  return `${value.slice(0, 8)}…${value.slice(-4)}`;
}
